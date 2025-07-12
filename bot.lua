
local function log(msg, ...)
    local ts = os.date("%Y-%m-%d %H:%M:%S")
    if select('#', ...) > 0 then
        msg = string.format(msg, ...)
    end
    print(string.format("[%s] %s", ts, msg))
end

io.stdout:setvbuf("no")
log("=== СТАРТ БОТА ===")

local script_path = arg[0]:match("(.*[/\\])") or "./"
if not script_path:match("[/\\]$") then
    script_path = script_path .. "/"
end
log("Определен путь к скрипту: %s", script_path)

local function list_parsers()
    local parsers_dir = script_path .. "../Parsers/parsers/"
    local files = {}
    local is_windows = package.config:sub(1,1) == '\\'
    local list_cmd = is_windows and ('dir /b "'..parsers_dir..'"') or ('ls "'..parsers_dir..'" 2>/dev/null')
    local p = io.popen(list_cmd)
    if p then
        for folder in p:lines() do
            local parser_file = parsers_dir .. folder .. "/parser.py"
            local f = io.open(parser_file, "r")
            if f then
                f:close()
                table.insert(files, {name = folder, path = parser_file})
            end
        end
        p:close()
    end
    return files
end


local token = os.getenv("BOT_TOKEN") or '8077528195:AAGSA33_AWyyRbr47GLDzz7PB2aQKtUG95I'
log("Используется токен: %s", token)

log("Загрузка библиотек...")
local socket = require("socket")
local json = require("cjson")
log("Библиотеки загружены")

local running_commands = {}
local API_URL = "https://api.telegram.org/bot" .. token

local function file_exists(path)
    local f = io.open(path, "r")
    if f then f:close() return true end
    return false
end

local function fix_encoding(str)
    if not str then return "" end
    return str:gsub('[^\32-\126\128-\255\n\t\r]', '')
end

local function telegram_request(method, params)
    local json_params = json.encode(params)
    local command = string.format('curl -s -X POST -H "Content-Type: application/json" -d \'%s\' "%s/%s"',
        json_params,
        API_URL, method
    )
    local response = io.popen(command):read("*a")
    local success, data = pcall(json.decode, response)
    if success and data and data.ok then
        return data.result
    else
        log("Ошибка запроса: %s", response)
        return nil
    end
end

local function safe_send_document(chat_id, filepath, caption)
    chat_id = math.floor(tonumber(chat_id))
    log("Попытка отправки документа в chat_id: %d", chat_id)

    local command = string.format(
        'curl -s -F "chat_id=%d" -F "document=@%s" -F "caption=%s" "%s/sendDocument"',
        chat_id, filepath, caption or "", API_URL
    )

    local response = io.popen(command):read("*a")
    local success, data = pcall(json.decode, response)
    if success and data and data.ok then
        return data.result
    else
        log("Ошибка отправки документа: %s", response)
        return nil
    end
end

local function safe_send(chat_id, text, disable_markdown)
    chat_id = math.floor(tonumber(chat_id))
    log("Попытка отправки сообщения в chat_id: %d", chat_id)
    text = fix_encoding(text)
    if #text == 0 then
        text = "ℹ️ Сообщение пустое после обработки"
    end

    local params = {
        chat_id = chat_id,
        text = text
    }

    if not disable_markdown and not text:match("[_*`%[%](){}<>#+=|!-]") then
        params.parse_mode = "Markdown"
    end

    return telegram_request("sendMessage", params)
end

local function exec_cmd(cmd, timeout, check_cancel)
    log("Выполняется команда: %s", cmd)
    local start_time = socket.gettime()
    local result = ""

    local f = io.popen(cmd .. " 2>&1", "r")
    if not f then
        log("Ошибка запуска команды: %s", cmd)
        return "Ошибка запуска команды"
    end

    f:setvbuf("no")

    while true do
        local chunk = f:read("*l")
        if chunk then
            chunk = fix_encoding(chunk)
            result = result .. chunk .. "\n"
        end

        if check_cancel and check_cancel() then
            log("Команда отменена пользователем")
            os.execute("pkill -f '" .. cmd:gsub("'", "'\\''") .. "'")
            result = result .. "\n[ПРЕРВАНО: Отменено пользователем]"
            break
        end

        if timeout and (socket.gettime() - start_time) > timeout then
            log("Превышен таймаут выполнения команды")
            os.execute("pkill -f '" .. cmd:gsub("'", "'\\''") .. "'")
            result = result .. "\n[ПРЕРВАНО: Превышен таймаут]"
            break
        end

        if not chunk then break end
        socket.sleep(0.1)
    end

    f:close()
    return result
end

local function list_python_files_in_tests()
    local test_dir = script_path .. "tests/"
    local files = {}

    local is_windows = package.config:sub(1,1) == '\\'
    local check_cmd = is_windows and ('if exist "'..test_dir..'" (echo exists)') or ('test -d "'..test_dir..'" && echo exists')
    local p_check = io.popen(check_cmd, "r")
    local exists = p_check:read("*l")
    p_check:close()
    if exists ~= "exists" then
        log("Папка tests не найдена по пути: %s", test_dir)
        return files, test_dir
    end

    local list_cmd = is_windows and ('dir /b "'..test_dir..'"') or ('ls "'..test_dir..'" 2>/dev/null')
    local p = io.popen(list_cmd)
    if p then
        for file in p:lines() do
            if file:match("%.py$") and file ~= "health_check.py" then
                table.insert(files, file)
            end
        end
        p:close()
    else
        log("Не удалось открыть папку tests для чтения")
    end

    return files, test_dir
end

local function on_callback_query(callback, chat_id)
    local data = callback.data
    log("Получен callback_query от chat_id %d: %s", chat_id, data or "")

    if data:match("^run_") then
        local script_name = data:match("^run_(.+)")
        if script_name then
            local script_file = script_path .. "tests/" .. script_name
            if not file_exists(script_file) then
                script_file = script_file .. ".py"
                if not file_exists(script_file) then
                    log("Скрипт не найден: %s", script_file)
                    safe_send(chat_id, "❌ Скрипт не найден: " .. script_name)
                    telegram_request("answerCallbackQuery", {
                        callback_query_id = callback.id,
                        text = "Скрипт не найден!"
                    })
                    return
                end
            end

            telegram_request("answerCallbackQuery", {
                callback_query_id = callback.id,
                text = "Запускаю скрипт..."
            })

            if running_commands[chat_id] then
                log("Отменяем предыдущую команду для chat_id %d", chat_id)
                running_commands[chat_id].cancel = true
                socket.sleep(1)
            end

            safe_send(chat_id, "🚀 Запускаю скрипт: " .. script_name)
            running_commands[chat_id] = { cancel = false }

            local check_cancel = function()
                return running_commands[chat_id] and running_commands[chat_id].cancel
            end

            local res = exec_cmd('python3 "' .. script_file .. '"', 300, check_cancel)

            if running_commands[chat_id] and running_commands[chat_id].cancel then
                safe_send(chat_id, "❌ Выполнение скрипта отменено")
            else
                local temp_file = os.tmpname() .. ".txt"
                local f = io.open(temp_file, "w")
                if f then
                    f:write(res)
                    f:close()

                    safe_send_document(
                        chat_id,
                        temp_file,
                        "✅ Результат выполнения " .. script_name
                    )

                    os.remove(temp_file)
                else
                    safe_send(chat_id, "❌ Ошибка создания временного файла")
                end
            end

            running_commands[chat_id] = nil
        end
        return
    end

    if data:match("^runparser_") then
        local parser_name = data:match("^runparser_(.+)")
        local parser_file = script_path .. "../Parsers/parsers/" .. parser_name .. "/parser.py"
        if not file_exists(parser_file) then
            safe_send(chat_id, "❌ Парсер не найден: " .. parser_name)
            telegram_request("answerCallbackQuery", {
                callback_query_id = callback.id,
                text = "Парсер не найден!"
            })
            return
        end

        telegram_request("answerCallbackQuery", {
            callback_query_id = callback.id,
            text = "Запускаю парсер..."
        })

        if running_commands[chat_id] then
            running_commands[chat_id].cancel = true
            socket.sleep(1)
        end

        safe_send(chat_id, "🚀 Запускаю парсер: " .. parser_name)
        running_commands[chat_id] = { cancel = false }

        local check_cancel = function()
            return running_commands[chat_id] and running_commands[chat_id].cancel
        end

        local res = exec_cmd('python3 "' .. parser_file .. '"', 300, check_cancel)

        if running_commands[chat_id] and running_commands[chat_id].cancel then
            safe_send(chat_id, "❌ Выполнение парсера отменено")
        else
            local temp_file = os.tmpname() .. ".txt"
            local f = io.open(temp_file, "w")
            if f then
                f:write(res)
                f:close()
                safe_send_document(
                    chat_id,
                    temp_file,
                    "✅ Результат работы парсера " .. parser_name
                )
                os.remove(temp_file)
            else
                safe_send(chat_id, "❌ Ошибка создания временного файла")
            end
        end

        running_commands[chat_id] = nil
        return
    end

end

local function on_message(message)
    local text = (message.text or ""):lower()
    local chat_id = message.chat and message.chat.id
    if not chat_id then
        log("Ошибка: message.chat.id отсутствует")
        return
    end

    chat_id = math.floor(tonumber(chat_id))
    log("Получено сообщение от chat_id %d: %s", chat_id, text)

    if text == "/stop" then
        if running_commands[chat_id] then
            log("Получена команда остановки для chat_id %d", chat_id)
            running_commands[chat_id].cancel = true
            safe_send(chat_id, "⏹️ Останавливаю выполнение команд...")
        else
            safe_send(chat_id, "ℹ️ Нет активных команд для остановки")
        end
        return
    elseif text == "/scripts" then
        local py_files, tests_dir = list_python_files_in_tests()
        if #py_files == 0 then
            safe_send(chat_id, "Нет доступных Python-скриптов в папке tests.")
            return
        end

        local keyboard = {inline_keyboard = {}}
        for _, file in ipairs(py_files) do
            local script_name = file:gsub("%.py$", "")
            table.insert(keyboard.inline_keyboard, {
                {text = script_name, callback_data = "run_" .. script_name}
            })
        end

        telegram_request("sendMessage", {
            chat_id = chat_id,
            text = "Выберите скрипт для запуска из папки tests:",
            reply_markup = keyboard
        })
        return
    elseif text == "/parsers" then
    local parsers = list_parsers()
    if #parsers == 0 then
        safe_send(chat_id, "Нет доступных парсеров.")
        return
    end
    local keyboard = {inline_keyboard = {}}
    for _, parser in ipairs(parsers) do
        table.insert(keyboard.inline_keyboard, {
            {text = parser.name, callback_data = "runparser_" .. parser.name}
        })
    end
    telegram_request("sendMessage", {
        chat_id = chat_id,
        text = "Выберите парсер для запуска:",
        reply_markup = keyboard
    })
    return
    elseif text == "/checkdbconn" then
        local health_check_path = script_path .. "health_check.py"
        if not file_exists(health_check_path) then
            safe_send(chat_id, "❌ Скрипт проверки БД не найден")
            return
        end

        safe_send(chat_id, "🔌 Проверка подключения к БД...")
        local res = exec_cmd('python3 "' .. health_check_path .. '" dbconn', 15, nil)
        safe_send(chat_id, "🔌 Результат проверки подключения к БД:\n" .. res, true)
        return

    elseif text == "/checkdata" then
        local health_check_path = script_path .. "health_check.py"
        if not file_exists(health_check_path) then
            safe_send(chat_id, "❌ Скрипт проверки данных не найден")
            return
        end

        if running_commands[chat_id] then
            log("Отменяем предыдущую команду для chat_id %d", chat_id)
            running_commands[chat_id].cancel = true
            socket.sleep(0.5)
        end

        running_commands[chat_id] = { cancel = false }
        safe_send(chat_id, "🔍 Запускаю анализ данных...")

        local check_cancel = function()
            return running_commands[chat_id] and running_commands[chat_id].cancel
        end

        local res = exec_cmd('python3 "' .. health_check_path .. '" data', 300, check_cancel)

        if running_commands[chat_id] and running_commands[chat_id].cancel then
            safe_send(chat_id, "❌ Анализ данных отменен")
        else
            safe_send(chat_id, "📊 Результат анализа данных:\n" .. res, true)
        end

        running_commands[chat_id] = nil
        return
    elseif text == "/start" then
        safe_send(chat_id, "Доступные команды:\n" ..
            "/scripts - список скриптов\n" ..
            "/checkdbconn - проверка подключения к БД\n" ..
            "/checkdata - анализ данных на пропуски\n" ..
            "/stop - отменить выполнение")
        return
    else
        log("Сообщение не распознано: %s", text)
        safe_send(chat_id, "ℹ️ Используйте /start для просмотра доступных команд")
    end
end

local function run_bot()
    log("Бот запущен и готов к работе")
    local offset = 0

    telegram_request("getUpdates", {
        offset = -1,
        timeout = 0
    })

    while true do
        local updates = telegram_request("getUpdates", {
            offset = offset,
            timeout = 30,
            allowed_updates = {"message", "callback_query"}
        })

        if updates then
            for _, update in ipairs(updates) do
                offset = update.update_id + 1

                if update.message then
                    on_message(update.message)
                end

                if update.callback_query and update.callback_query.message then
                    local chat_id = update.callback_query.message.chat.id
                    chat_id = math.floor(tonumber(chat_id))
                    on_callback_query(update.callback_query, chat_id)
                end
            end
        else
            log("Ошибка получения обновлений")
        end

        socket.sleep(0.5)
    end
end

run_bot()