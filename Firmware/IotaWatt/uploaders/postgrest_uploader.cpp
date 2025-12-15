#include "postgrest_uploader.h"
#include "splitstr.h"

/*****************************************************************************************
 * PostgREST uploader implementation
 *
 * Uploads IoTaWatt sensor data to PostgreSQL/TimescaleDB via PostgREST API.
 * PostgREST provides automatic RESTful endpoints for PostgreSQL tables.
 *
 * Expected database schema:
 * 
 * CREATE TABLE <your table name> (
 *   timestamp TIMESTAMPTZ NOT NULL,
 *   device TEXT NOT NULL,
 *   sensor TEXT NOT NULL,
 * 
 *   One or more of the following:
 * 
 *   Watts  DOUBLE PRECISION,
 *   Amps   DOUBLE PRECISION,
 *   PF     DOUBLE PRECISION,
 *   VA     DOUBLE PRECISION,
 *   VAR    DOUBLE PRECISION,
 *   Volts  DOUBLE PRECISION,
 *   Hz     DOUBLE PRECISION,    
 *      
 * );
 *
 ***************************************************************************************/

/*****************************************************************************************
 * Configuration parsing
 ***************************************************************************************/
bool postgrest_uploader::configCB(JsonObject &Json)
{
    trace(T_postgrest, 90);
    if (Json.containsKey("table"))
    {
        delete[] _table;
        _table = charstar(Json["table"].as<char *>());
    }
    else
    {
        log("%s: table name required", _id);
        return false;
    }

    delete[] _deviceName;
    _deviceName = charstar(Json["deviceName"] | "$device");

    delete[] _schema;
    _schema = charstar(Json["schema"] | "public");
    if(!strlen(_schema))
    {
        delete[] _schema;
        _schema = charstar("public");
    }

    delete[] _jwtToken;
    _jwtToken = nullptr;
    if (Json.containsKey("jwtToken"))
    {
        _jwtToken = charstar(Json["jwtToken"].as<char *>());
    }
    
    // sort the measurements by name then units so they can be combined into single rows
    
    trace(T_postgrest, 90, 5);    
    _outputs->sort([this](Script* a, Script* b)->int {
        int comp = strcmp(a->name(), b->name());
        if(comp)
        {
            return comp;
        }
        return a->getUnitsEnum() - b->getUnitsEnum();
    });
    
        // Create list of units used.
    
    trace(T_postgrest, 90, 6);
    for (int i = 0; i < unitsCount; i++)
    {
        _unit_active[i] = false;
    }
    Script *script = _outputs->first();
    while(script)
    {
        _unit_active[script->getUnitsEnum()] = true;
        script = script->next();
    }
    String CSVheader = "timestamp,device,sensor";
    for (int i = 0; i < unitsCount; i++)
    {
        if(_unit_active[i])
        {
            CSVheader += ',';
            CSVheader += unitstr[i];
        }
    }
    delete[] _CSVheader;
    _CSVheader = charstar(CSVheader);
    
    // Log successful configuration with key details
    
    trace(T_postgrest, 90, 7);
    log("%s: Configured for table %s.%s %s", _id, _schema, _table,
        _jwtToken ? "with JWT auth" : "(anonymous)");
        
    trace(T_postgrest, 90, 9);    
    return true;
}

/*****************************************************************************************
 * Parse PostgreSQL timestamp to UNIX timestamp
 * PostgreSQL timestamps can possibility include timezone offsets (although not encouraged!)
 * A few postgresSQL formats are allowed.
 ***************************************************************************************/
uint32_t postgrest_uploader::parseTimestamp(const char *timestampStr)
{
    int year, month, day, hour, minute, second;
    int tzHours = 0, tzMinutes = 0;
    char tzSign = '+';

    // PostgreSQL with timezone: "2023-10-15 14:30:25+10:30"
    if (sscanf(timestampStr, "%d-%d-%d %d:%d:%d%c%d:%d",
               &year, &month, &day, &hour, &minute, &second, &tzSign, &tzHours, &tzMinutes) == 9)
    {

        int32_t tzOffsetSeconds = (tzHours * 3600) + (tzMinutes * 60);
        if (tzSign == '-')
        {
            tzOffsetSeconds = -tzOffsetSeconds;
        }

        uint32_t utcTime = Unixtime(year, month, day, hour, minute, second);
        return utcTime - tzOffsetSeconds;
    }

    // PostgreSQL with hour-only timezone: "2023-10-15 14:30:25+10"
    if (sscanf(timestampStr, "%d-%d-%d %d:%d:%d%c%d",
               &year, &month, &day, &hour, &minute, &second, &tzSign, &tzHours) == 8)
    {

        int32_t tzOffsetSeconds = tzHours * 3600;
        if (tzSign == '-')
        {
            tzOffsetSeconds = -tzOffsetSeconds;
        }

        uint32_t utcTime = Unixtime(year, month, day, hour, minute, second);
        return utcTime - tzOffsetSeconds;
    }

    // ISO 8601 UTC: "2023-10-15T14:30:25Z"
    if (sscanf(timestampStr, "%d-%d-%dT%d:%d:%dZ", &year, &month, &day, &hour, &minute, &second) == 6)
    {
        return Unixtime(year, month, day, hour, minute, second);
    }

    // ISO 8601 without timezone: "2023-10-15T14:30:25"
    if (sscanf(timestampStr, "%d-%d-%dT%d:%d:%d", &year, &month, &day, &hour, &minute, &second) == 6)
    {
        return Unixtime(year, month, day, hour, minute, second);
    }

    // Simple format: "2023-10-15 14:30:25"
    if (sscanf(timestampStr, "%d-%d-%d %d:%d:%d", &year, &month, &day, &hour, &minute, &second) == 6)
    {
        return Unixtime(year, month, day, hour, minute, second);
    }

    return 0;
}

/*****************************************************************************************
 * Query database for last uploaded timestamp to determine resume point
 * This is a query to the REST API (not a direct SQL query)
 ***************************************************************************************/
uint32_t postgrest_uploader::handle_query_s()
{
    trace(T_postgrest, 10);
    _lastSent = 0;
    String endpoint = "/";
    if (_schema && strcmp(_schema, "public") != 0)
    {
        endpoint += _schema;
        endpoint += ".";
    }
    endpoint += _table;
    endpoint += "?select=timestamp&device=eq.";
    endpoint += resolveDeviceName();
    endpoint += "&order=timestamp.desc&limit=1";

    if (!WiFi.isConnected())
    {
        return UTCtime() + 1;
    }

    trace(T_postgrest, 10,1);
    HTTPGet(endpoint.c_str(), checkQuery_s);
    trace(T_postgrest, 10,2);
    return 1;
}

/*****************************************************************************************
 * Process query response to set upload resume point
 ***************************************************************************************/
uint32_t postgrest_uploader::handle_checkQuery_s()
{

    trace(T_postgrest, 20);
    // Check if async request is complete
    if (!_request || _request->readyState() != 4)
    {
        return 10;
    }

    delete[] _statusMessage;
    _statusMessage = nullptr;

    int httpCode = _request->responseHTTPcode();
    String responseText = _request->responseText();
    trace(T_postgrest, 20,1);
    if (httpCode != 200)
    {
        trace(T_postgrest, 20, 2);
        char message[responseText.length()+50];
        if (httpCode < 0)
        {
            snprintf_P(message, 50, PSTR("Query failed, code %d"), httpCode);
        }
        else
        {
            snprintf_P(message, responseText.length()+50, PSTR("Query failed, code %d, response: %s"), httpCode, responseText.c_str());
        }
        _statusMessage = charstar(message);

        // Clean up failed request
        delete _request;
        _request = nullptr;
        // Use same as influx V1 method
        delay(5, query_s);
        return 15;
    }

    trace(T_postgrest, 20, 3);
    DynamicJsonBuffer JsonBuffer;
    JsonArray &jsonArray = JsonBuffer.parseArray(responseText);

    if (jsonArray.success() && jsonArray.size() > 0)
    {
        trace(T_postgrest, 20, 4);    
        JsonObject &lastRecord = jsonArray[0];
        if (lastRecord.containsKey("timestamp"))
        {
            String timestampStr = lastRecord["timestamp"].as<String>();
            uint32_t timestamp = parseTimestamp(timestampStr.c_str());
            if (timestamp > 0)
            {
                _lastSent = timestamp;
            }
        }
    }
    
    trace(T_postgrest, 20, 5);
    _lastSent = MAX(_lastSent, _uploadStartDate);
    _lastSent = MAX(_lastSent, Current_log.firstKey());
    _lastSent -= _lastSent % _interval;

    if (!_stop)
    {
        log("%s: Start posting at %s", _id, localDateString(_lastSent + _interval).c_str());
    }

    // Clean up successful request
    delete _request;
    _request = nullptr;
    _state = write_s;
    trace(T_postgrest, 20, 6);
    return 1;
}

/*****************************************************************************************
 * Build JSON payload and upload to PostgREST
 * Note multiple units (e.g. Watts, PF, Amps, Volts) on a single sensor are not supported.
 * The UI reflects this (i.e. you can't select multiple units for a sensor).
 * TODO: Allow multiple units per sensor.
 ***************************************************************************************/
uint32_t postgrest_uploader::handle_write_s()
{
    trace(T_postgrest, 30);
    if (_stop)
    {
        stop();
        return 1;
    }

    uint32_t dataThreshold = _lastSent + _interval + (_interval * _bulkSend);

    if (Current_log.lastKey() < dataThreshold)
    {
        if (oldRecord)
        {
            delete oldRecord;
            oldRecord = nullptr;
            delete newRecord;
            newRecord = nullptr;
        }
        return UTCtime() + 1;
    }

    if (!oldRecord)
    {
        oldRecord = new IotaLogRecord;
        newRecord = new IotaLogRecord;
        newRecord->UNIXtime = _lastSent + _interval;
        Current_log.readKey(newRecord);
    }

    if (reqData.available() == 0)
    {
        reqData.print(_CSVheader);
    }

    while (reqData.available() < uploaderBufferLimit &&
           newRecord->UNIXtime < Current_log.lastKey())
    {
        trace(T_postgrest,30,1);

        if (micros() > bingoTime)
        {
            // Don't hog the CPU
            return 10; 
        }

        IotaLogRecord *swap = oldRecord;
        oldRecord = newRecord;
        newRecord = swap;
        newRecord->UNIXtime = oldRecord->UNIXtime + _interval;
        Current_log.readKey(newRecord);

        double elapsedHours = newRecord->logHours - oldRecord->logHours;
        if (elapsedHours == 0)
        {
            if ((newRecord->UNIXtime + _interval) <= Current_log.lastKey())
            {
                return 1;
            }
            return UTCtime() + 1;
        }

        // Format timestamp as UTC-with-a-TZ for PostgreSQL TIMESTAMPTZ

        String timestampStr = datef(oldRecord->UNIXtime, "YYYY-MM-DDThh:mm:ssZ");

        // Process the output scripts and build sensor rows with all requested units.

        trace(T_postgrest,30,3);    
        Script *script = _outputs->first();
        String sensor = script->name();
        reqData.printf("\n%s,%s,%s",
                        timestampStr.c_str(),
                        resolveDeviceName().c_str(),
                        sensor.c_str());
        int unitIndex = 0;

        trace(T_postgrest,30,4);    
        while (script)
        {
            trace(T_postgrest,30,6);
            double value = script->run(oldRecord, newRecord);
            if (value == value){

                // If sensor changed

                if( ! sensor.equals(script->name()))
                {
                    trace(T_postgrest,30,7);

                    // Finish row.

                    while(unitIndex < unitsCount)
                    {
                       if(_unit_active[unitIndex++])
                       {
                           reqData.print(",NULL");
                       } 
                    }

                    // Start a new row

                    sensor = script->name();
                    reqData.printf("\n%s,%s,%s",
                        timestampStr.c_str(),
                        resolveDeviceName().c_str(),
                        sensor.c_str());
                    unitIndex = 0;
                }
                trace(T_postgrest,30,8);

                // Fill null units

                while(unitIndex < script->getUnitsEnum())
                {
                    if(_unit_active[unitIndex++])
                       {
                           reqData.print(",NULL");
                       } 
                }

                // Output this script value if appropriate (ignores a duplicate unit).

                if(unitIndex == script->getUnitsEnum())
                {
                    trace(T_postgrest,30,9);
                    reqData.printf(",%.*f", script->precision(), value);
                    unitIndex++;
                }
            }
            script = script->next();
        }

        // Finish row.

        trace(T_postgrest,30,10);    
        while(unitIndex < unitsCount)
        {
            if(_unit_active[unitIndex++])
            {
                reqData.print(",NULL");
            } 
        }
        
        _lastPost = oldRecord->UNIXtime;
    }
    reqData.print('\n');
    
    if (reqData.available() <= 1)
    {
        reqData.flush();
        delete oldRecord;
        oldRecord = nullptr;
        delete newRecord;
        newRecord = nullptr;
        return UTCtime() + 5;
    }
    
    delete oldRecord;
    oldRecord = nullptr;
    delete newRecord;
    newRecord = nullptr;
    
    String endpoint = "/";
    if (_schema && strcmp(_schema, "public") != 0)
    {
        endpoint += _schema;
        endpoint += ".";
    }
    endpoint += _table;
    
    trace(T_postgrest,30,11);
    HTTPPost(endpoint.c_str(), checkWrite_s, "text/csv");
    return 1;
}

/*****************************************************************************************
 * Process upload response
 ***************************************************************************************/
uint32_t postgrest_uploader::handle_checkWrite_s()
{
    if (!_request)
    {
        return 10;
    }

    // Handle connection failures (readyState != 4) as failures requiring cleanup
    if (_request->readyState() != 4)
    {

        delete _request;
        _request = nullptr;
        _state = write_s;
        return UTCtime() + 10;
    }

    delete[] _statusMessage;
    _statusMessage = nullptr;
    
    // PostgREST returns 201 for successful inserts (vs InfluxDB's 204)

    int httpCode = _request->responseHTTPcode();
    if (httpCode == 201)
    {
        _lastSent = _lastPost;
        _state = write_s;
        return 1;
    }
    
    String responseText = _request->responseText();
    char message[responseText.length()+50];
    if (httpCode < 0)
    {
        snprintf_P(message, 50, PSTR("POST failed, code %d"), httpCode);
    }
    else
    {
        snprintf_P(message, responseText.length()+50, PSTR("POST failed, code %d, response: %s"), httpCode, responseText.c_str());
    }
    _statusMessage = charstar(message);

        
    // Deal with failure - follow InfluxDB v1 pattern
    
    delete _request;
    _request = nullptr;
    _state = write_s;
    return UTCtime() + 10;
}

void postgrest_uploader::setRequestHeaders()
{
    String prefer = "return=minimal";
    if (_jwtToken)
    {
        String auth = "Bearer ";
        auth += _jwtToken;
        _request->setReqHeader("Authorization", auth.c_str());
    }
}

int postgrest_uploader::scriptCompare(Script *a, Script *b)
{
    return strcmp(a->name(), b->name());
}

/*****************************************************************************************
 * Resolve device name with variable substitution
 ***************************************************************************************/
String postgrest_uploader::resolveDeviceName()
{
    if (!_deviceName)
    {
        return String(deviceName);
    }

    String result = String(_deviceName);

    if (result.indexOf("$device") >= 0)
    {
        result.replace("$device", String(deviceName));
    }

    return result;
}
