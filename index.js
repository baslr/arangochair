'use strict';

const EventEmitter = require('events');

const https = require('request-easy').https;
const http = require('request-easy').http;
const url = require('url');

class ArangoChair extends EventEmitter {

    constructor(adbUrl) {

        super();

        adbUrl = url.parse(adbUrl);
        this.req = new (adbUrl.protocol === 'https:' ? https : http)({
            hostname: adbUrl.hostname,
            port: adbUrl.port
        });

        const db = '/' === adbUrl.pathname ? '/_system' : adbUrl.pathname;

        this._loggerStatePath = `/_db${db}/_api/replication/logger-state`;
        this._loggerFollowPath = `/_db${db}/_api/replication/logger-follow`;

        this.collectionsMap = new Map();
        this._stopped = false;
    }

    start() {
        this._stopped = false;
        this._startLoggerState();
    }

    stop() {
        this._stopped = true;
    }

    _startLoggerState() {

        this.req.get({path: this._loggerStatePath}, (status, headers, body) => {

            if (200 !== status) {

                this.emit('error', new Error('E_LOGGERSTATE'), status, headers, body);
                this.stop();

                return;
            }

            body = JSON.parse(body);

            let lastLogTick = body.state.lastLogTick;
            let type;
            let tid;
            let entry;

            const txns = new Map();

            const handleEntry = () => {

                const colName = entry.cname;

                const colConf = this.collectionsMap.get(colName);
                if (undefined === colConf) return;

                const events = colConf.get('events');
                const event = this.inferEventType(tid, type);

                if (0 !== events.size && !events.has(event)) return;

                const key = entry.data._key;
                const keys = colConf.get('keys');

                if (0 !== keys.size && !events.has(key)) return;

                const doc = entry.data;

                this.emit(colName, doc, typeText);
            };

            const ticktock = () => {

                if (this._stopped) return;

                this.req.get({path: `${this._loggerFollowPath}?from=${lastLogTick}`}, (status, headers, body) => {

                    if (204 < status || 0 === status) {
                        this.emit('error', new Error('E_LOGGERFOLLOW'), status, headers, body);
                        this.stop();
                        return;
                    } // if

                    if ('0' === headers['x-arango-replication-lastincluded']) {
                        return setTimeout(ticktock, 500);
                    } // if

                    lastLogTick = headers['x-arango-replication-lastincluded'];

                    const entries = body.toString().trim().split('\n');

                    for (const i in entries) {

                        entry = JSON.parse(entries[i]);

                        // transaction   {"tick":"514132959101","type":2200,"tid":"514132959099","database":"1"}
                        // insert/update {"tick":"514092205556","type":2300,"tid":"0","database":"1","cid":"513417247371","cname":"test","data":{"_id":"test/testkey","_key":"testkey","_rev":"514092205554",...}}
                        // delete        {"tick":"514092206277","type":2302,"tid":"0","database":"1","cid":"513417247371","cname":"test","data":{"_key":"abcdef","_rev":"514092206275"}}

                        type = entry.type;
                        tid = entry.tid;

                        if (2200 === type) { // txn start
                            txns.set(tid, new Set());
                        } else if (2201 === type) { // txn commit and replay docs
                            for (const data of txns.get(tid)) {
                                [type, entry] = data;
                                handleEntry();
                            } // for
                            txns.delete(tid);
                        } else if (2002 === type) { // txn abort
                            txns.delete(tid);
                        } else {

                            if (2300 !== type && 2302 !== type) continue;

                            if ('0' !== tid) {
                                txns.get(tid).add([type, entry]);
                                continue;
                            }

                            handleEntry();
                        }
                    }
                    ticktock();
                });
            };
            ticktock();
        });
    }

    inferEventType(tid, type) {

        if (type === 2300) {    // type 2300 means insert or update
            return tid === '0' ? "insert" : "update";    // the tid tells us which of the above it is
        }

        if (type === 2302) {     // type 2302 means delete
            return "delete";
        }
    }

    subscribe(confs) {

        if ('string' === typeof confs) confs = {collection: confs};
        if (!Array.isArray(confs)) confs = [confs];

        for (const conf of confs) {

            let colConfMap = undefined;

            if (this.collectionsMap.has(conf.collection)) {
                colConfMap = this.collectionsMap.get(conf.collection);
            } else {
                colConfMap = new Map([['events', new Set()], ['keys', new Set()]]);
                this.collectionsMap.set(conf.collection, colConfMap);
            }

            if (conf.events) {
                for (const event of conf.events) {
                    colConfMap.get('events').add(event);
                }
            }

            if (conf.keys) {
                for (const key of conf.keys) {
                    colConfMap.get('keys').add(key);
                }
            }
        }
    }

    unsubscribe(confs) {

        if ('string' === typeof confs) confs = {collection: confs};
        if (!Array.isArray(confs)) confs = [confs];

        for (const conf of confs) {
            if (conf.events) {
                const events = this.collectionsMap.get(conf.collection).get('events');
                for (const event of conf.events) {
                    events.delete(event);
                }
            }
            if (conf.keys) {
                const keys = this.collectionsMap.get(conf.collection).get('keys');
                for (const key of conf.keys) {
                    keys.delete(key);
                }
            }

            if (!conf.events && !conf.keys) {
                this.collectionsMap.delete(conf.collection);
            }
        }
    }
}

module.exports = ArangoChair;
