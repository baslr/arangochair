'use strict';

const fastango = require('fastango3');
const assert   = require('assert');
const EventEmitter = require('events');

const https = require('request-easy').https; 
const http  = require('request-easy').http;
const url  = require('url');


const mapTextToType = {
    'insert/update':'2300',
    'delete':'2302'
};
const mapTypeToText = {
    '2300': 'insert/update',
    '2302': 'delete'
};



class ArangoChair extends EventEmitter {
    constructor(adbUrl) {
        super();
        adbUrl = url.parse(adbUrl);
        this.req = new (adbUrl.protocol === 'https:'? https : http)({
            hostname:adbUrl.hostname,
            port:adbUrl.port
        });

        const db = '/' === adbUrl.pathname ? '/_system' : adbUrl.pathname;

        this._loggerStatePath  = `/_db${db}/_api/replication/logger-state`;
        this._loggerFollowPath = `/_db${db}/_api/replication/logger-follow`;

        this.collectionsMap = new Map();
        this._stopped = false;

        this._liveQueries = new Map();
    }

    start() {
        this._stopped = false;
        this._startLoggerState();
    }

    stop() {
        this._stopped = true;
    }

    liveQuery(aql, collections, opts, callback) {
        if ('function' === typeof opts) {
            callback = opts;
            opts = {};
        } // if
        const fast = fastango('http://192.168.2.103:8529');
        let   res = [];
        const update = () => {
            fast._q(aql, {},{all:true,profile:true}, (status, results, extras) => {
                console.log(extras.stats.executionTime * 1000);
                if (! opts.diff) {
                    try {
                        if (res.length !== results.length) throw 'unequal';
                        for(const idx in results) {
                            assert.deepStrictEqual(results[idx], res[idx]);
                        } // for
                    } catch(e) {
                        res = results;
                        callback(res);
                    } // catch()
                } else { // if

                    const diff = [];

                    for(const result of results) {
                        let eq = false;
                        for(const r of res) {
                            try {
                                assert.notDeepStrictEqual(result, r);
                            } catch(e) {
                                eq = true;
                                break;
                            }
                        } // for
                        if (!eq) {
                            diff.push(result);
                        }
                    } // for

                    if (diff.length) {
                        res = results;
                        callback(diff);
                    } // if
                } // else
            });
        };
        if (opts.initialResultSet) update();
        this._liveQueries.set(aql, {collections:collections, callback:update});

/*        for(const collection of collections) {
            this.subscribe(collection);
            this.on(collection, update);
        } // for */
    }

    _startLoggerState() {
        this.req.get({path:this._loggerStatePath}, (status, headers, body) => {
            if (200 !== status) {
                this.emit('error', new Error('E_LOGGERSTATE'), status, headers, body);
                this.stop();
                return;
            } // if

            body = JSON.parse(body);
            let lastLogTick = body.state.lastLogTick;
            let start = 0;
            let idx   = 0;

            let type  = 0;
            let tid   = 0;
            let entry = 0;

            let typeStartIdx = 0;
            let typeEndIdx   = 0;
            let idx0 = 0;
            let idx1 = 0;

            const typeStartBuffer       = Buffer.from('type":');
            const cnameStartBuffer      = Buffer.from('cname":"');
            const keyStartBuffer        = Buffer.from('_key":"');
            const commaDoubleTickBuffer = Buffer.from(',"');

            const txns = new Map();
            const cols = new Set();

            const handleEntry = () => {
                idx0 = entry.indexOf(cnameStartBuffer, idx0 + 2) + 8;
                idx1 = entry.indexOf(commaDoubleTickBuffer, idx0) - 1;

                const colName = entry.slice(idx0, idx1).toString();
                if ('0' !== tid) {
                    cols.add(colName);
                } else {
                    for(const [key, m] of this._liveQueries) {
                        // console.log(m);
                        if (m.done) continue;
                        if (~m.collections.indexOf(colName)) {
                            m.callback();
                            m.done = true;
                        } // if
                        m.done = false;
                    } // for
                } // else

                const colConf = this.collectionsMap.get(colName);
                if (undefined === colConf) return;

                const events = colConf.get('events');

                if (0 !== events.size && !events.has(type)) return;

                idx0 = entry.indexOf(keyStartBuffer, idx1 + 9);
                const key = entry.slice(idx0+7, entry.indexOf(commaDoubleTickBuffer, idx0+7)-1).toString();
                const keys = colConf.get('keys');

                if (0 !== keys.size && !events.has(key)) return;

                this.emit(colName, entry.slice(idx1 + 9, -1), mapTypeToText[type]);
            };

            const ticktock = () => {
                if (this._stopped) return;

                this.req.get({path:`${this._loggerFollowPath}?from=${lastLogTick}`}, (status, headers, body) => {
                    if (204 < status || 0 === status) {
                        this.emit('error', new Error('E_LOGGERFOLLOW'), status, headers, body);
                        this.stop();
                        return;
                    } // if

                    if ('0' === headers['x-arango-replication-lastincluded']) {
                        return setTimeout(ticktock, 500);
                    } // if

                    lastLogTick = headers['x-arango-replication-lastincluded'];

                    start = idx = 0;
                    while(true) {
                        idx = body.indexOf('\n', start);
                        if (-1 === idx) break;

                        entry = body.slice(start, idx);
                        start = idx+1;


                        console.log(entry.toString() );

                        // transaction   {"tick":"514132959101","type":2200,"tid":"514132959099","database":"1"}
                        // insert/update {"tick":"514092205556","type":2300,"tid":"0","database":"1","cid":"513417247371","cname":"test","data":{"_id":"test/testkey","_key":"testkey","_rev":"514092205554",...}}
                        // delete        {"tick":"514092206277","type":2302,"tid":"0","database":"1","cid":"513417247371","cname":"test","data":{"_key":"abcdef","_rev":"514092206275"}}

                        idx0 = entry.indexOf(typeStartBuffer) + 6;            // find type":
                        idx1 = entry.indexOf(commaDoubleTickBuffer, idx0);    // find ,"
                        type = entry.slice(idx0, idx1).toString();

                        idx0 = entry.indexOf(commaDoubleTickBuffer, idx1+8) - 1; // find ,"
                        tid  = entry.slice(idx1+8, idx0).toString();

                        if ('2200' === type) { // txn start
                            txns.set(tid, new Set());
                            for(const m of this._liveQueries) m.done = false;

                        } else if ('2201' === type) { // txn commit and replay docs
                            for(const data of txns.get(tid)) {
                                idx0 = 0;
                                [type, entry] = data;
                                handleEntry();
                            } // for
                            txns.delete(tid);

                            for(const [key, m] of this._liveQueries) {
                                // console.log(m);

                                for(const col of cols) {
                                    if (m.done) continue;
                                    if (~m.collections.indexOf(col)) {
                                        m.callback();
                                        m.done = true;
                                    }
                                } // for
                                m.done = false;
                            } // for
                            cols.clear();

                        } else if ('2002' === type) { // txn abort
                            txns.delete(tid);
                            cols.clear();

                        } else {
                            if ('2300' !== type && '2302' !== type) continue;

                            if ('0' !== tid) {
                                txns.get(tid).add([type,entry.slice(idx0+14)]);
                                continue;
                            } // if

                            handleEntry();
                        } // else
                    } // while
                    ticktock();
                });
            }
            ticktock();
        });
    }

    subscribe(confs) {
        if ('string' === typeof confs) confs = {collection:confs};
        if (!Array.isArray(confs))     confs = [confs];

        for(const conf of confs) {
            let colConfMap = undefined;
            if (this.collectionsMap.has(conf.collection)) {
                colConfMap = this.collectionsMap.get(conf.collection);
            } else {
                colConfMap = new Map([ ['events', new Set()],['keys', new Set()] ]);
                this.collectionsMap.set(conf.collection, colConfMap);
            }

            if (conf.events) {
                for(const event of conf.events) {
                    colConfMap.get('events').add(mapTextToType[event]);
                } // for
            } // if
            if (conf.keys) {
                for(const key of conf.keys) {
                    colConfMap.get('keys').add(key);
                } // for
            } // if
        } // for
    } // subscribe()

    unsubscribe(confs) {
        if ('string' === typeof confs) confs = {collection:confs};
        if (!Array.isArray(confs))     confs = [confs];

        for(const conf of confs) {
            if (conf.events) {
                const events = this.collectionsMap.get(conf.collection).get('events');
                for(const event of conf.events) {
                    events.delete(mapTextToType[event]);
                } // for
            } // if
            if (conf.keys) {
                const keys = this.collectionsMap.get(conf.collection).get('keys');
                for(const key of conf.keys) {
                    keys.delete(key);
                } // for
            }// if

            if (!conf.events && !conf.keys) {
                this.collectionsMap.delete(conf.collection);
            } // if
        } // for
    } // unsubscribe()
}


module.exports = ArangoChair;
