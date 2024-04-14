# @themost/oracle
Most Web Framework Oracle Adapter
## Install

```bash
npm install @themost/oracle
```

## Usage

Register Oracle data adapter at app.json:

```json
{
    "adapterTypes": [
        { "name":"Oracle Data Adapter", "invariantName": "oracle", "type":"@themost/oracle" }
    ],
    adapters: [
        { "name":"development", "invariantName":"oracle", "default":true,
            "options": {
              "host":"localhost",
              "port":1521,
              "user":"user",
              "password":"password",
              "service":"orcl",
              "schema":"PUBLIC"
            }
        }
    ]
}
```

Use `session` options to define Oracle session parameters. For example:

```json
{ 
    "name":"development", "invariantName":"oracle", "default":true,
    "options": {
        "host":"localhost",
        "port":1521,
        "user":"user",
        "password":"password",
        "service":"orcl",
        "schema":"PUBLIC",
        "session": {
            "NLS_COMP": "LINGUISTIC",
            "NLS_SORT": "BINARY_CI"
        }
    }
}
```

If you are intended to use Oracle adapter as the default database adapter set the property "default" to true.

 Note: Most Web Framework Oracle Adapter depends on [Oracle Database driver for Node.js](https://github.com/oracle/node-oracledb) maintained by Oracle Corp.
 
 Before install it, read the node-oracledb [installation instructions] (https://github.com/oracle/node-oracledb/blob/master/INSTALL.md) provided by Oracle Corp.
