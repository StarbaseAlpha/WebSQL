'use strict';

function WebSQL(dbName = null, size = 1024 * 1024 * 2) {

  let run = async (statement = null, values = []) => {
    return new Promise((resolve, reject) => {
      let db = openDatabase(dbName, 1, dbName, size || 1024 * 1024 * 2);
      db.transaction(tx => {
        tx.executeSql(statement || null, values || [], (t, r) => {
          r.tx = t;
          resolve(r);
        }, (t, e) => {
          reject({
            "message": "Error executing SQL statement.",
            "error": e,
            "tx": t
          });
        });
      });
    });
  };

  let datastore = (tableName) => {

    let store = {};
    let created = false;

    let onEvent = null;

    const eventHandler = (e) => {
      if (onEvent && typeof onEvent === 'function') {
        onEvent(e);
      }
    };

    const createStore = async () => {
      let create = await run("create table if not exists " + tableName + " (key text primary key not null unique, value text)");
      created = true;
      return create;
    };

    store.onEvent = (cb) => {
      onEvent = cb;
    };

    store.put = async (key, value) => {
      if (!created) {
        await createStore();
      }
      return new Promise(async (resolve, reject) => {
        if (!key || typeof key !== 'string') {
          return reject({
            "code": 400,
            "message": "A key is required."
          });
        }
        let val = JSON.stringify(value);
        let write = await run("replace into " + tableName + " (key, value) values (?,?)", [key, val]).catch(err => {
          return null;
        });
        if (write) {
          let e = {
            "event": "write",
            "key": key,
            "timestamp": Date.now()
          };
          eventHandler(e);
          resolve(e);
        } else {
          reject({
            "code": 400,
            "message": "Could not write data to key."
          });
        }
      });
    };

    store.get = async (key) => {
      if (!created) {
        await createStore();
      }
      return new Promise(async (resolve, reject) => {
        let read = await run("select * from " + tableName + " where key = ?", [key]).catch(err => {
          return null;
        });
        let result = {
          "key": key,
          "value": null
        };
        let rows = Array.from(read.rows);
        if (rows && rows.length > 0) {
          result.value = JSON.parse(rows[0].value || null);
        }
        resolve(result);
      });
    };

    store.del = async (keys) => {
      if (!created) {
        await createStore();
      }
      if (!keys) {
        return Promise.reject({
          "code": 400,
          "message": "A key or an array of keys is required."
        });
      }
      let keyIds = [];
      if (typeof keys === 'string') {
        keyIds = [keys];
      } else {
        keyIds = keys;
      }
      let ks = Array.from(new Uint8Array(keyIds.length)).map(val => {
        return "?";
      }).join(',');
      let del = await run("delete from " + tableName + " where key in (" + ks + ")", keyIds).catch(err => {
        console.log(err);
        return null;
      });
      let e = {
        "event": "delete",
        "keys": keyIds,
        "timestamp": Date.now()
      };
      eventHandler(e);
      return e;
    };

    store.list = async (query = {}) => {
      if (!created) {
        await createStore();
      }
      let statement = "select key ";
      if (query.values) {
        statement += ", value ";
      }
      statement += "from " + tableName + " ";
      let values = [];
      if (query.lt || query.gt) {
        statement += "where ";
        if (query.lt) {
          statement += "key < ? ";
          values.push(query.lt.toString());
        }
        if (query.gt) {
          if (values[0]) {
            statement += "and ";
          }
          statement += "key > ? ";
          values.push(query.gt.toString());
        }
      }
      statement += "order by key ";
      if (query && query.reverse) {
        statement += "desc ";
      }
      if (query && query.limit && !isNaN(query.limit)) {
        statement += "limit " + parseInt(query.limit) + " ";
      }
      let result = await run(statement, values).catch(err => {
        console.log(err);
        return null;
      });
      let rows = Array.from(result.rows).map(row => {
        let key = row.key.toString();
        let item = key;
        if (query.values) {
          let value = JSON.parse(row.value);
          item = {
            "key": key,
            "value": value
          };
        }
        return item;
      });
      return rows;
    };

    store.importDB = async (dbArray) => {
      if (!created) {
        await createStore();
      }
      if (!dbArray || dbArray.length < 1) {
        let e = {"db":tableName, "event":"importDB", "keys":[], "timestamp": Date.now()};
        eventHandler(e);
        return e;
      }
      let statement = "replace into " + tableName + "(key, value) values ";
      let values = [];
      let keys = [];
      for(let i = 0; i < dbArray.length; i++) {
        statement += "(?,?), ";
        values.push(dbArray[i].key.toString());
        values.push(JSON.stringify(dbArray[i].value));
        keys.push(dbArray[i].key);
      }
      statement = statement.slice(0, -2);
      let imported = await sql.run(statement, values);
      let e = {"db": tableName, "event": "importDB", "keys": keys, "timestamp": Date.now()};
      eventHandler(e);
      return e;
    };

    store.exportDB = async () => {
      if (!created) {
        await createStore();
      }
      return store.list({"deep":true, "values":true});
    };

    store.deleteDB = async () => {
      if (!created) {
        await createStore();
      }
      return sql.run("drop table if exists " + tableName).then(result => {
        let e = {
          "db": tableName,
          "event": "deleteDB",
          "timestamp": Date.now()
        };
        created = false;
        eventHandler(e);
        return e;
      });
    };

    return store;

  };

  return {
    run,
    datastore
  };

}
