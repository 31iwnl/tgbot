local json = require('cjson')
local cURL = require("cURL")
local date = require("date")
local log = require('utils.log')
local config = require('config.espocms')

local _M = {}
_M.result = nil
_M.base = config.url..'api/v1/'
log.outfile = 'logs/espocms_'..os.date('%Y-%m-%d')..'.log' 
log.level = 'trace'	
_M.defaults = {
	['filter'] = {},
	['maxSize'] = 5,
	['offset'] = 0,
	['orderBy'] = 'number',
	['order'] = 'desc'	
}

function query(data)
	if not data then return '' end
	local str = _M.auth
	for k,v in pairs(data) do str = str..'&'..k..'='..v end
	return str
end

function get_result(str,url)
	local result, err = pcall(json.decode,str)
	if result then
		_M.result = json.decode(str)
	else				
		log.error(url..':'..err)
		return nil,	err
	end	
	return _M.result
end

function get(url,data)
	local str = ''
	url =  _M.base..url..query(data)
	local headers = {
		'Content-type: application/json',
		'Accept: application/json',
		'X-Api-Key: '..config.key
	}
	local c = cURL.easy{		
		url = url,	
		httpheader  = headers,	
		writefunction = function(st)	
			str = str..st
		end
	}
	local ok, err = c:perform()	
	c:close()
	if not ok then return nil, err end
	local res,err = get_result(str,url)
	if not res then return nil,err end
	return res
end

function post(url,data)
	local str = ''
	url = _M.base..url
	local headers = {
		'Content-Type: application/json',
		'Accept: application/json',
		'X-Api-Key: '..config.key
	}
	local c = cURL.easy{		
		url = url,
		post = true,
		postfields =  json.encode(data),  
		httpheader  = headers,
		writefunction = function(st)		
			str = str..st
		end
	}
	local ok, err = c:perform()	
	c:close()
	if not ok then return nil, err end
	local res,err = get_result(str,url)
	if not res then return nil,err end
	return res
end

function put(url,data)
	local str = ''
	url = _M.base..url
	local headers = {
		'Content-Type: application/json',
		'Accept: application/json',
		'X-Api-Key: '..config.key
	}
	local c = cURL.easy{		
		url = url,
		put = true,
		postfields =  json.encode(data),  
		httpheader  = headers,
		writefunction = function(st)		
			str = str..st
		end
	}
	local ok, err = c:perform()	
	c:close()
	if not ok then return nil, err end
	local res,err = get_result(str,url)
	if not res then return nil,err end
	return res
end


function delete(url)
	local str = ''
	url = _M.base..url
	local headers = {
		'Content-Type: application/json',
		'Accept: application/json',
		'X-Api-Key: '..config.key
	}
	local c = cURL.easy{		
		url = url,
		httpheader  = headers,
		customrequest = 'DELETE',
		writefunction = function(st)		
			str = str..st
		end
	}
	local ok, err = c:perform()	
	c:close()
	if not ok then return nil, err end
	local res,err = get_result(str,url)
	if not res then return nil,err end
	return res
end

function _M.list(typ,searchParams)
	if not searchParams then searchParams = _M.defaults end
	return get(typ,{['searchParams']=json.encode(searchParams)})
end

function _M.read(typ,id)
	return get(typ..'/'..id)
end

function _M.create(typ,payload)
	return post(typ,payload)
end

function _M.update(typ,id,payload)
	return put(typ..'/'..id,payload)
end

function _M.delete(typ,id)
	return delete(typ..'/'..id)
end

return _M
