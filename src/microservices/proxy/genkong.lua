-----------------------------------------------------------------------------
-- lua generator for Kong Declarative config
-----------------------------------------------------------------------------
local lyaml = require 'lyaml'

local function env(varname, cast)
    if cast == nil then
        return os.getenv(varname)
    end

    return cast(os.getenv(varname))
end

local function target(str)
    if str == nil then
        return
    end
    local _, _, protocol, target = string.find(str, '([%a%d]+)://(.+)')
    return {
        protocol = protocol,
        target = target
    }
end

local is_gradual = env("GRADUAL_MIGRATION")
if is_gradual ~= nil then
    is_gradual = is_gradual == "true"
else
    is_gradual = false
end

local movies_url = target(env('MOVIES_SERVICE_URL'))
local monolith_url = target(env('MONOLITH_URL'))
local events_url = target(env('EVENTS_SERVICE_URL'))
if movies_url == nil or monolith_url == nil or events_url == nil then
    io.stderr:write('Env variables MOVIES_SERVICE_URL, MONOLITH_URL, \z
                     EVENTS_SERVICE_URL should be set!\n')
    os.exit(1)
end

local monolith_movies
local monolith_users = { target = monolith_url.target }
local movies = { target = movies_url.target }
local events = { target = events_url.target }
if is_gradual then
    local movies_weight = env("MOVIES_MIGRATION_PERCENT", tonumber)
    if movies_weight == nil or movies_weight <0 or movies_weight > 100 then
        io.stderr:write('Incorrect MOVIES_MIGRATION_PERCENT value. \z
                         Should be a number from 0 to 100\n')
        os.exit(1)
    end
    local monolith_movies_weight = 100 - movies_weight
    if monolith_movies_weight > 0 then
        monolith_movies = {
            target = monolith_url.target;
            weight = 100 - movies_weight
        }
        movies.weight = movies_weight
    end
end

print(lyaml.dump({{
    _format_version = "3.0";
    _transform = true;
    services = {
        {
            name = 'users-service';
            host = 'users-upstream';
            routes = {
                {
                    name = 'users-route';
                    strip_path = false;
                    paths = {
                        '/api/users'
                    }
                }
            }
        },
        {
            name = 'movies-service';
            host = 'movies-upstream';
            routes = {
                {
                    name = 'movies-route';
                    strip_path = false;
                    paths = {
                        '/api/movies'
                    }
                }
            }
        };
        {
            name = 'events-service';
            host = 'events-upstream';
            routes = {
                {
                    name = 'events-route';
                    strip_path = false;
                    paths = {
                        '/api/events'
                    }
                }
            }
        }
    },
    upstreams = {
        {
            name = 'movies-upstream';
            targets = {
                movies;
                monolith_movies
            }
        };
        {
            name = 'events-upstream';
            targets = {
                events
            }
        },
        {
            name = 'users-upstream';
            targets = {
                monolith_users
            }
        }
    }
}}))
