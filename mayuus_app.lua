-- app configuration
local server_settings = {
    host = '0.0.0.0',
    port = 8080,
    log_requests = true,
    log_errors = true,
    display_errors = false,
    max_connections = 3, -- per second
    allow_get_all = true, -- get all values by GET /kv
}

local log = require 'log'
local json = require 'json'

local pkg = {}

pkg.start_server = function()
    local server_options = {
        log_requests = server_settings.log_requests,
        log_errors = server_settings.log_errors,
        display_errors = server_settings.display_errors,
    }
    local server = require 'http.server'.new(server_settings.host, server_settings.port, server_options)
    local router = require 'http.router'.new{ charset = 'application/json' }
    server:set_router(router)

    local function check_busyness()
        if server_settings.max_connections and server_settings.max_connections > 0 then
            log.info(json.encode{ max_connections = server_settings.max_connections })
            local bus = box.space.kv_connections
            local msec = math.floor(require 'clock'.realtime() * 1000)
            log.info(json.encode{ msec = msec })
            bus:insert{ nil, msec }
            for _, tuple in bus.index.msec:pairs(msec-999, { iterator = box.index.LT }) do
                bus:delete(tuple[1])
            end
            local count = bus:len()
            if count > server_settings.max_connections then
                return false
            end
        end
        return true
    end
    
    local function log_success(method, path, body)
        if method and path then
            local message = { method, ' ', path, ' succeded' }
            if body then
                table.insert(message, ' with body: '..json.encode(body))
            end
            log.info(table.concat(message))
        end
    end

    local function log_fail(method, path, err)
        if method and path then
            log.info(table.concat{ method, ' ', path, ' failed: ', err or '' })
        end
    end

    local function resp_too_many_reqs(method, path)
        local err_title = 'Too Many Requests'
        log_fail(method, path, err_title)
        return {
            body = json.encode{ message = err_title },
            status = 429,
        }
    end

    local function resp_not_found(method, path)
        local err_title = 'Not Found'
        log_fail(method, path, err_title)
        return {
            body = json.encode{ message = err_title },
            status = 404,
        }
    end

    local function resp_bad_req(method, path, description)
        local err_title = 'Bad Request: '..description
        log_fail(method, path, err_title)
        return {
            body = json.encode{ message = err_title },
            status = 400,
        }
    end

    local handlers = {}

    -- POST /kv body: {key: "test", "value": {SOME ARBITRARY JSON}}
    -- POST возвращает 409 если ключ уже существует
    -- POST возвращают 400 если боди некорректное
    handlers.post = function(req)
        if not check_busyness() then
            return resp_too_many_reqs()
        end
        local body = req:json()
        log.info(json.encode{ body = body })
        local key = body.key
        local value = body.value
        if not key and not value then
            return resp_bad_req(req:method(), req:path(), "'key' and 'value' are expected")
        elseif not key then
            return resp_bad_req(req:method(), req:path(), "'key' is expected")
        elseif not value then
            return resp_bad_req(req:method(), req:path(), "'value' is expected")
        else
            key = tostring(key)
            local existing_tuple = box.space.kv.index.key:get(key, { iterator = box.index.EQ })
            if existing_tuple == nil then
                local result = box.space.kv:insert{ key, value }
                log_success(req:method(), req:path(), body)
                return {
                    body = json.encode{ message = 'OK', key = result[1], value = result[2] },
                    status = 201,
                }
            else
                local err_title = "Conflict: 'key' must be unique"
                log_fail(req:method(), req:path(), err_title)
                return {
                    body = json.encode{ message = err_title },
                    status = 409,
                }
            end
        end
    end

    -- PUT kv/{id} body: {"value": {SOME ARBITRARY JSON}}
    -- PUT возвращают 400 если боди некорректное
    -- PUT возвращает 404 если такого ключа нет
    handlers.put = function(req)
        if not check_busyness() then
            return resp_too_many_reqs(req:method(), req:path())
        end
        local key = req:stash('key')
        key = tostring(key)
        local body = req:json()
        log.info(json.encode{ stashed_key = key, body = body })
        local value = body.value
        if not value then
            return resp_bad_req(req:method(), req:path(), "'value' is expected")
        else
            local tuple = box.space.kv.index.key:get(key, { iterator = box.index.EQ })
            if tuple ~= nil then
                local result = box.space.kv:update(key, {{'=', 2, value}})
                log_success(req:method(), req:path(), body)
                return {
                    body = json.encode{ message = 'OK', key = result[1], value = result[2] },
                    status = 200,
                }
            else
                return resp_not_found(req:method(), req:path())
            end
        end
    end

    -- GET kv/{id}
    -- GET возвращает 404 если такого ключа нет
    handlers.get = function(req)
        if not check_busyness() then
            return resp_too_many_reqs(req:method(), req:path())
        end
        local key = req:stash('key')
        log.info(json.encode{ stashed_key = key })
        key = tostring(key)
        local tuple = box.space.kv.index.key:get(key, { iterator = box.index.EQ })
        log.info(json.encode{ found_tuple = tuple })
        if tuple ~= nil then
            return {
                body = json.encode{ message = 'OK', value = tuple[2] },
                status = 200,
            }
        else
            return resp_not_found(req:method(), req:path())
        end
    end

    -- DELETE kv/{id}
    -- DELETE возвращает 404 если такого ключа нет
    handlers.delete = function(req)
        local method = req:method()
        local path = req:path()
        if not check_busyness() then
            return resp_too_many_reqs(method, path)
        end
        local key = req:stash('key')
        log.info(json.encode{ stashed_key = key })
        key = tostring(key)
        local tuple = box.space.kv.index.key:get(key, { iterator = box.index.EQ })
        log.info(json.encode{ found_tuple = tuple })
        if tuple ~= nil then
            box.space.kv:delete(tuple[1])
            log.info(table.concat{ method, ' ', path, ' succeded' })
            return {
                body = json.encode{ message = 'OK' },
                status = 200,
            }
        else
            return resp_not_found(method, path)
        end
    end

    handlers.get_all = function(req)
        if not check_busyness() then
            return resp_too_many_reqs(req:method(), req:path())
        end
        local tuples = box.space.kv:select()
        return {
            body = json.encode{ message = 'OK', data = tuples },
            status = 200,
        }
    end

    router:route({ path = '/kv', method = 'POST' }, handlers.post)
    router:route({ path = '/kv/:key', method = 'PUT' }, handlers.put)
    router:route({ path = '/kv/:key', method = 'GET' }, handlers.get)
    router:route({ path = '/kv/:key', method = 'DELETE' }, handlers.delete)

    if server_settings.allow_get_all then
        router:route({ path = '/kv', method = 'GET' }, handlers.get_all)
    end

    server:start()
end

pkg.init = function()
    -- create/update kv space
    box.schema.create_space('kv', { if_not_exists = true })
    box.space.kv:create_index('key', {
        unique = true,
        if_not_exists = true,
        parts = { 1, 'string', is_nullable = false },
    })
    -- create/update space for logging request frequency
    box.schema.sequence.create('kv_connections', { start = 1, step = 1, if_not_exists = true })
    box.schema.create_space('kv_connections', { if_not_exists = true })
    box.space.kv_connections:create_index('id', {
        sequence = 'kv_connections',
        unique = true,
        if_not_exists = true,
        parts = { 1, 'unsigned', is_nullable = false },
    })
    box.space.kv_connections:create_index('msec', {
        unique = false,
        if_not_exists = true,
        parts = { 2, 'unsigned', is_nullable = false },
    })
    box.space.kv_connections:truncate()

    assert(box.space.kv_connections:len() == 0, 'Something went wrong while resetting kv_busyness')

    pkg.start_server()
end

return pkg
