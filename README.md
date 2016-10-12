# arangochair

`arangochair` pushs ArangoDB changes in realtime to you.

## install

```bash
npm install --save arangochair
```

## quick example

```es6
const arangochair = require('arangochair');

const no4 = new arangochair('http://127.0.0.1:8529/'); // ArangoDB node to monitor

const no4 = new arangochair('http://127.0.0.1:8529/myDb'); // ArangoDB node to monitor, with database name

no4.subscribe({collection:'users'});
no4.start();
no4.on('users', (doc, type) => {
    // do something awesome

    // doc:Buffer
    // type:'insert/update'|'delete'
});

no4.on('error', (err, httpStatus, headers, body) => {
    // arangochair stops on errors
    // check last http request
    no4.start();
});
```

## subscribe

```es6
// subscribe to all events in the users collection
no4.subscribe('users');

// explicit
no4.subscribe({collection:'users', events:['insert/update', 'delete']});


// subscribe the users collection with only the delete event
no4.subscribe({collection:'users', events:['delete']});

// subscribe the users collection with only the delete event on key myKey
no4.subscribe({collection:'users', events:['delete'], keys:['myKey']});
```

## unsubscribe
```es6
// unsubscribe the users collection
no4.unsubscribe('users');

// unsubscribe the delete event in the users collection
no4.unsubscribe({collection:'users',events:['delete']});

// unsubscribe the key myKey in the users collection
no4.unsubscribe({collection:'users',keys:['myKey']});
```
