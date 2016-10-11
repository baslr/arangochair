'use strict';

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
        this.req.get({path:'/_api/replication/logger-state'}, (status, headers, body) => {
            if (200 !== status) {
                this.emit('error', new Error('E_LOGGERSTATE'), status, headers, body);
                this.stop();
                return;
            } // if

            body = JSON.parse(body);
            let lastLogTick = body.state.lastLogTick;
            let start = 0;
            let idx   = 0;

            const ticktock = () => {
                if (this._stopped) return;

                this.req.get({path:`/_api/replication/logger-follow?from=${lastLogTick}`}, (status, headers, body) => {
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

                        const entry = body.toString('utf8', start, idx);
                        start = idx+1;

                        try {
                            // insert/update {"tick":"514092205556","type":2300,"tid":"0","database":"1","cid":"513417247371","cname":"test","data":{"_id":"test/testkey","_key":"testkey","_rev":"514092205554",...}}
                            // delete .      {"tick":"514092206277","type":2302,"tid":"0","database":"1","cid":"513417247371","cname":"test","data":{"_key":"abcdef","_rev":"514092206275"}}
                            /*
                                                                                                         operation type     transaction id      collection name  document id     optional select id (!in del ops)
                                                                                                         |                  |                   |                |               |               document key
                                                                                                         |                  |                   |                |               |               |              */
                            const [str, type, tid, colName, strFix, id, key] = entry.match(/\{.*type\"\:(.*?)\,\"tid\"\:\"(.*?)\".*cname\"\:\"(.*?)\"[^{]*(\{(\"_id\"\:\".*?\"\,)?\"\_key\"\:\"(.*?)\")/);
                            const strLen  = str.length - strFix.length;

                            const colConf = this.collectionsMap.get(colName);
                            if (undefined === colConf) continue;
                            const events = colConf.get('events');
                            const keys   = colConf.get('keys');

                            if ( (0 === events.size || events.has(type)) &&
                                 (0 === keys.size   || keys.has(key)) ) {
                                this.emit(colName, entry.slice(strLen, -1), mapTypeToText[type]);
                            } // if
                        }catch(e) {
                            ; // handle transactions 2200 / 2201
                        }
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
