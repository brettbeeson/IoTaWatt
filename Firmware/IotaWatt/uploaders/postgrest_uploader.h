#ifndef postgrest_uploader_h
#define postgrest_uploader_h

#include "IotaWatt.h"
#include "Uploader.h"

class postgrest_uploader : public Uploader 
{
    public:
        postgrest_uploader() : _table(0),
                               _deviceName(0),
                               _jwtToken(0),
                               _schema(0),
                               _CSVheader(nullptr)
        {
            _id = charstar("postgrest");
        };

        ~postgrest_uploader(){
            delete[] _table;
            delete[] _deviceName;
            delete[] _jwtToken;
            delete[] _schema;
        };

    protected:
        char *_table;           // Database table name
        char *_deviceName;      // Device identifier (supports $device substitution)
        char *_jwtToken;        // JWT token for authentication
        char *_schema;          // Database schema name
        bool _unit_active[unitsCount]; // true[] if unit used in output script
        char *_CSVheader;       // Header for CSV output
        
        uint32_t handle_query_s();
        uint32_t handle_checkQuery_s();
        uint32_t handle_write_s();
        uint32_t handle_checkWrite_s();
        bool configCB(JsonObject &);
        uint32_t parseTimestamp(const char* timestampStr);

        void setRequestHeaders();
        int scriptCompare(Script *a, Script *b);
        String resolveDeviceName();
};

#endif