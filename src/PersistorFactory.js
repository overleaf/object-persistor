const Settings = require('./Settings')
const Logger = require('logger-sharelatex')

module.exports = function create() {
  Logger.info(
    {
      backend: Settings.backend,
      fallback: Settings.fallback && Settings.fallback.backend
    },
    'Loading backend'
  )
  if (!Settings.backend) {
    throw new Error('no backend specified - config incomplete')
  }

  function getPersistor(backend) {
    switch (backend) {
      case 'aws-sdk':
      case 's3':
        return require('./S3Persistor')
      case 'fs':
        return require('./FSPersistor')
      case 'gcs':
        return require('./GcsPersistor')
      default:
        throw new Error(`unknown filestore backend: ${backend}`)
    }
  }

  let persistor = getPersistor(Settings.backend)

  if (Settings.fallback && Settings.fallback.backend) {
    const migrationPersistor = require('./MigrationPersistor')
    persistor = migrationPersistor(
      persistor,
      getPersistor(Settings.fallback.backend)
    )
  }

  return persistor
}
