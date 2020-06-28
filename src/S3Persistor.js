const http = require('http')
const https = require('https')
if (http.globalAgent.maxSockets < 300) {
  http.globalAgent.maxSockets = 300
}
if (https.globalAgent.maxSockets < 300) {
  https.globalAgent.maxSockets = 300
}

const AbstractPersistor = require('./AbstractPersistor')
const PersistorHelper = require('./PersistorHelper')

const fs = require('fs')
const S3 = require('aws-sdk/clients/s3')
const { URL } = require('url')
const Stream = require('stream')
const { promisify } = require('util')
const {
  WriteError,
  ReadError,
  NotFoundError,
  SettingsError
} = require('./Errors')
const pipeline = promisify(Stream.pipeline)

module.exports = class S3Persistor extends AbstractPersistor {
  constructor(settings = {}) {
    super()

    this.settings = settings
  }

  async sendFile(bucketName, key, fsPath) {
    return this.sendStream(bucketName, key, fs.createReadStream(fsPath))
  }

  async sendStream(bucketName, key, readStream, sourceMd5) {
    try {
      // egress from us to S3
      const observeOptions = {
        metric: 's3.egress',
        Metrics: this.settings.Metrics
      }
      let b64Hash

      if (sourceMd5) {
        b64Hash = PersistorHelper.hexToBase64(sourceMd5)
      } else {
        // if there is no supplied md5 hash, we calculate the hash as the data passes through
        observeOptions.hash = 'md5'
      }

      const observer = new PersistorHelper.ObserverStream(observeOptions)
      pipeline(readStream, observer)

      // if we have an md5 hash, pass this to S3 to verify the upload
      const uploadOptions = {
        Bucket: bucketName,
        Key: key,
        Body: observer
      }
      if (b64Hash) {
        uploadOptions.ContentMD5 = b64Hash
      }

      const response = await this._getClientForBucket(bucketName)
        .upload(uploadOptions, { partSize: this.settings.partSize })
        .promise()
      let destMd5 = S3Persistor._md5FromResponse(response)
      if (!destMd5) {
        // the eTag isn't in md5 format so we need to calculate it ourselves
        const verifyStream = await this.getObjectStream(
          response.Bucket,
          response.Key,
          {}
        )
        destMd5 = await PersistorHelper.calculateStreamMd5(verifyStream)
      }

      // if we didn't have an md5 hash, we should compare our computed one with S3's
      // as we couldn't tell S3 about it beforehand
      if (!sourceMd5) {
        sourceMd5 = observer.getHash()
        // throws on mismatch
        await PersistorHelper.verifyMd5(
          this,
          bucketName,
          key,
          sourceMd5,
          destMd5
        )
      }
    } catch (err) {
      throw PersistorHelper.wrapError(
        err,
        'upload to S3 failed',
        { bucketName, key },
        WriteError
      )
    }
  }

  async getObjectStream(bucketName, key, opts) {
    opts = opts || {}

    const params = {
      Bucket: bucketName,
      Key: key
    }
    if (opts.start != null && opts.end != null) {
      params.Range = `bytes=${opts.start}-${opts.end}`
    }

    const stream = this._getClientForBucket(bucketName)
      .getObject(params)
      .createReadStream()

    // ingress from S3 to us
    const observer = new PersistorHelper.ObserverStream({
      metric: 's3.ingress',
      Metrics: this.settings.Metrics
    })

    try {
      // wait for the pipeline to be ready, to catch non-200s
      await PersistorHelper.getReadyPipeline(stream, observer)
      return observer
    } catch (err) {
      throw PersistorHelper.wrapError(
        err,
        'error reading file from S3',
        { bucketName, key, opts },
        ReadError
      )
    }
  }

  async getRedirectUrl() {
    // not implemented
    return null
  }

  async deleteDirectory(bucketName, key, continuationToken) {
    let response
    const options = { Bucket: bucketName, Prefix: key }
    if (continuationToken) {
      options.ContinuationToken = continuationToken
    }

    try {
      response = await this._getClientForBucket(bucketName)
        .listObjectsV2(options)
        .promise()
    } catch (err) {
      throw PersistorHelper.wrapError(
        err,
        'failed to list objects in S3',
        { bucketName, key },
        ReadError
      )
    }

    const objects = response.Contents.map((item) => ({ Key: item.Key }))
    if (objects.length) {
      try {
        await this._getClientForBucket(bucketName)
          .deleteObjects({
            Bucket: bucketName,
            Delete: {
              Objects: objects,
              Quiet: true
            }
          })
          .promise()
      } catch (err) {
        throw PersistorHelper.wrapError(
          err,
          'failed to delete objects in S3',
          { bucketName, key },
          WriteError
        )
      }
    }

    if (response.IsTruncated) {
      await this.deleteDirectory(
        bucketName,
        key,
        response.NextContinuationToken
      )
    }
  }

  async getObjectSize(bucketName, key) {
    try {
      const response = await this._getClientForBucket(bucketName)
        .headObject({ Bucket: bucketName, Key: key })
        .promise()
      return response.ContentLength
    } catch (err) {
      throw PersistorHelper.wrapError(
        err,
        'error getting size of s3 object',
        { bucketName, key },
        ReadError
      )
    }
  }

  async getObjectMd5Hash(bucketName, key) {
    try {
      const response = await this._getClientForBucket(bucketName)
        .headObject({ Bucket: bucketName, Key: key })
        .promise()
      return S3Persistor._md5FromResponse(response)
    } catch (err) {
      throw PersistorHelper.wrapError(
        err,
        'error getting hash of s3 object',
        { bucketName, key },
        ReadError
      )
    }
  }

  async deleteObject(bucketName, key) {
    try {
      await this._getClientForBucket(bucketName)
        .deleteObject({ Bucket: bucketName, Key: key })
        .promise()
    } catch (err) {
      // s3 does not give us a NotFoundError here
      throw PersistorHelper.wrapError(
        err,
        'failed to delete file in S3',
        { bucketName, key },
        WriteError
      )
    }
  }

  async copyObject(bucketName, sourceKey, destKey) {
    const params = {
      Bucket: bucketName,
      Key: destKey,
      CopySource: `${bucketName}/${sourceKey}`
    }
    try {
      await this._getClientForBucket(bucketName).copyObject(params).promise()
    } catch (err) {
      throw PersistorHelper.wrapError(
        err,
        'failed to copy file in S3',
        params,
        WriteError
      )
    }
  }

  async checkIfObjectExists(bucketName, key) {
    try {
      await this.getObjectSize(bucketName, key)
      return true
    } catch (err) {
      if (err instanceof NotFoundError) {
        return false
      }
      throw PersistorHelper.wrapError(
        err,
        'error checking whether S3 object exists',
        { bucketName, key },
        ReadError
      )
    }
  }

  async directorySize(bucketName, key, continuationToken) {
    try {
      const options = {
        Bucket: bucketName,
        Prefix: key
      }
      if (continuationToken) {
        options.ContinuationToken = continuationToken
      }
      const response = await this._getClientForBucket(bucketName)
        .listObjectsV2(options)
        .promise()

      const size = response.Contents.reduce((acc, item) => item.Size + acc, 0)
      if (response.IsTruncated) {
        return (
          size +
          (await this.directorySize(
            bucketName,
            key,
            response.NextContinuationToken
          ))
        )
      }
      return size
    } catch (err) {
      throw PersistorHelper.wrapError(
        err,
        'error getting directory size in S3',
        { bucketName, key },
        ReadError
      )
    }
  }

  _getClientForBucket(bucket, clientOptions) {
    if (this.settings.bucketCreds && this.settings.bucketCreds[bucket]) {
      return new S3(
        this._buildClientOptions(
          this.settings.bucketCreds[bucket],
          clientOptions
        )
      )
    }

    // no specific credentials for the bucket
    if (this.settings.key) {
      return new S3(this._buildClientOptions(null, clientOptions))
    }

    throw new SettingsError({
      message: 'no bucket-specific or default credentials provided',
      info: { bucket }
    })
  }

  _buildClientOptions(bucketCredentials) {
    const options = {}

    if (bucketCredentials) {
      options.credentials = {
        accessKeyId: bucketCredentials.auth_key,
        secretAccessKey: bucketCredentials.auth_secret
      }
    } else {
      options.credentials = {
        accessKeyId: this.settings.key,
        secretAccessKey: this.settings.secret
      }
    }

    if (this.settings.endpoint) {
      const endpoint = new URL(this.settings.endpoint)
      options.endpoint = this.settings.endpoint
      options.sslEnabled = endpoint.protocol === 'https'
    }

    // path-style access is only used for acceptance tests
    if (this.settings.pathStyle) {
      options.s3ForcePathStyle = true
    }

    return options
  }

  static _md5FromResponse(response) {
    const md5 = (response.ETag || '').replace(/[ "]/g, '')
    if (!md5.match(/^[a-f0-9]{32}$/)) {
      return null
    }

    return md5
  }
}
