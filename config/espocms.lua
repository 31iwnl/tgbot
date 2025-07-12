local config = require('config.config')
local _M = config
_M.file = 'espocms.conf' -- файл конфигурации
_M.read()
return _M.data
