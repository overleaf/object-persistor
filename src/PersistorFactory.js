const Logger = require('logger-sharelatex')
const { SettingsError } = require('./Errors')
const GcsPersistor = require('./GcsPersistor')
const S3Persistor = require('./S3Persistor')
const FSPersistor = require('./FSPersistor')
const MigrationPersistor = require('./MigrationPersistor')

function getPersistor(backend, settings) {
  switch (backend) {
    case 'aws-sdk':
    case 's3':
      return new S3Persistor(
        Object.assign({}, settings.s3, { Metrics: settings.Metrics })
      )
    case 'fs':
      return new FSPersistor({
        paths: settings.paths,
        Metrics: settings.Metrics
      })
    case 'gcs':
      return new GcsPersistor(
        Object.assign({}, settings.gcs, { Metrics: settings.Metrics })
      )
    default:
      throw new SettingsError({ message: 'unknown backend', info: { backend } })
  }
}

module.exports = function create(settings) {
  Logger.info(
    {
      backend: settings.backend,
      fallback: settings.fallback && settings.fallback.backend
    },
    'Loading backend'
  )
  if (!settings.backend) {
    throw new SettingsError({
      message: 'no backend specified - config incomplete',
      info: {}
    })
  }

  let persistor = getPersistor(settings.backend, settings)

  if (settings.fallback && settings.fallback.backend) {
    const primary = persistor
    const fallback = getPersistor(settings.fallback.backend, settings)
    persistor = new MigrationPersistor(
      primary,
      fallback,
      Object.assign({}, settings.fallback, { Metrics: settings.Metrics })
    )
  }

  return persistor
}
