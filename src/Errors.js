const OError = require('@overleaf/o-error')

class NotFoundError extends OError {}
class WriteError extends OError {}
class ReadError extends OError {}
class SettingsError extends OError {}

module.exports = {
  NotFoundError,
  WriteError,
  ReadError,
  SettingsError
}
