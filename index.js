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
            body = JSON.parse(body);
            let lastLogTick = body.state.lastLogTick;
            let start = 0;
            let idx   = 0;

            const ticktock = () => {
                if (this._stopped) return;

                this.req.get({path:`/_api/replication/logger-follow?from=${lastLogTick}`}, (status, headers, body) => {
                    if ('0' === headers['x-arango-replication-lastincluded']) {
                        return setTimeout(ticktock, 500);
                    } // if

                    lastLogTick = headers['x-arango-replication-lastincluded'];

                    start = idx = 0;
                    while(true) {
                        idx = body.indexOf('\n', start);
                        if (-1 === idx) break;

                        const txt   = body.toString('utf8', start, idx);
                        start = idx+1;

                        try {
                            const [str, type, colName] = txt.match(/\{.*type\"\:(.*?),.*cname\"\:\"(.*?)\"[^{]*/);

                            const pushMap = this.collectionsMap.get(colName);

                            if (undefined === pushMap) continue; 

                            if (!pushMap || pushMap.has(type)) {
                                this.emit(colName, txt.slice(str.length, -1), mapTypeToText[type]);
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
        if (!Array.isArray(confs)) confs = [confs];

        for(const conf of confs) {
            this.collectionsMap.set(conf.collection, false);

            if (conf.events) {
                this.collectionsMap.set(conf.collection, new Set());
                for(const push of conf.events) {
                    this.collectionsMap.get(conf.collection).add(mapTextToType[push]);
                } // for
            } // if
        } // for
    } // subscribe()

    unsubscribe(collection) {
        this.collectionsMap.delete(collection);
    }
}


module.exports = ArangoChair;