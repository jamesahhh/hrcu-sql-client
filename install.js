var Service = require('node-windows').Service

var svc = new Service({
    name: 'HRCU SQL Client',
    description: 'A Windows service for remote access to SQL Server',
    script: './index.js',
    env: {
        name: 'NODE_ENV',
        value: 'production',
    },
})

svc.on('install', function () {
    svc.start()
})

svc.on('alreadyinstalled', function () {
    console.log('This service is already installed.')
})

svc.on('start', function () {
    console.log(svc.name + ' started!')
})

svc.install()
