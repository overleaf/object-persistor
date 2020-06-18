const Settings = require('./src/Settings')
const PersistorFactory = require('./src/PersistorFactory')

module.exports = function ObjectPersistor(config) {
  Object.assign(Settings, config)
  return PersistorFactory()
}
module.exports.Errors = require('./src/Errors')
