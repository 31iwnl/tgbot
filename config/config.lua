local json = require('cjson.safe')
local _M = {}
_M.data = {}
_M.comments = {}
_M.file = '' -- файл конфигурации
local key

function _M.read()
	for line in io.lines(_M.file) do 	
		key = string.match(line, '([%w_]+)::')
		if (key) then 		
			_M.data[key] = string.match(line, '::(.*) #')	
			_M.comments[key] = string.match(line, '#(.*)')		
			if string.find(_M.data[key],'%{%"') or string.find(_M.data[key],'%[%"') then
				_M.data[key] = json.decode(_M.data[key])	
			end		
		end
	end
end

function reprint(k,v)
	if type(v) == 'table' then
		for i,j in pairs(v) do
			reprint(i,j)
		end
	else
		print(k..': '..v)
	end
end

function _M.data:write()
    local config_file = io.open(_M.file, 'w')
    for k,v in pairs(self) do
		if type(v) ~= 'function' then
			if type(v) == 'table' then v = json.encode(v) end
			config_file:write(k..'::'..v..' #'.._M.comments[key]..'\n')
		end
    end
	config_file:close() 
end

function _M.data:print()
	for k,v in pairs(self) do
		if type(v) ~= 'function' then	
			if _M.comments[k] then print('\n'.._M.comments[k]:gsub("^%s*(.-)%s*$", "%1")..': \n') end
			reprint(k,v)
		end
	end 
end

return _M

