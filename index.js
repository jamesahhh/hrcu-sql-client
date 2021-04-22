require('dotenv').config()
const { basename } = require('path')
const { unlink, access } = require('fs/promises')
const { watch } = require('chokidar')
const { constants, readFileSync, writeFile, mkdir } = require('fs')
const sql = require('mssql')
const { stringify } = require('querystring')

const log = console.log.bind(console)

checkFor(process.env.WATCH_DIRECTORY)
checkFor(process.env.OUTPUT_DIRECTORY)

var watcher = watch(process.env.WATCH_DIRECTORY, {
    awaitWriteFinish: {
        stabilityThreshold: 1500,
        pollInterval: 100,
    },
    ignored: /^(?=.*(\.\w+)$)(?!.*(\.txt)$).*$/,
})

watcher.on('add', (path) => fileOps(path))

function fileOps(path) {
    var array = readFileSync(path).toString().split(/\r?\n/)
    var ext = basename(path)
    connectDB(array, ext)
    removeFile(path)
}

function connectDB(array, ext) {
    var config = buildConfig(array)
    if (array[0] == 'GetAccount') {
        sql.connect(config)
            .then((pool) => {
                return pool
                    .request()
                    .input('input_1', sql.Char, array[3])
                    .input('input_2', sql.Char, array[4])
                    .query(`SELECT RTRIM(CUENTA) AS CUENTA, RTRIM(CLIENTEID) AS CLIENTEID, RTRIM(DESCRIPCION) AS DESCRIPCION,
                RTRIM(NOMBRE) AS NOMBRE, RTRIM(APELLIDO) AS APPELIDO, VALOR1, VALOR2, VALOR3, VALOR4, VALOR5, SOCIAL
                FROM ${config.table}
                WHERE (CUENTA=@input_1 AND CLIENTEID=@input_2) OR (CUENTA=@input_1)`)
            })
            .then((result) => {
                if (result.recordsets[0].length == 0) {
                    writeToFile('Data Source Invalid', ext)
                } else {
                    writeToFile(JSON.stringify(result.recordsets[0]), ext)
                }
            })
            .catch((err) => {
                log(err)
            })
    } else if (array[0] == 'SetPayment') {
        sql.connect(config)
            .then((pool) => {
                return pool
                    .request()
                    .input('input_1', sql.Char, array[3])
                    .input('input_2', sql.Char, array[4])
                    .input('input_3', sql.Char, array[5])
                    .input('input_4', sql.Money, array[6])
                    .input('input_5', sql.Char(1), array[7])
                    .input('input_6', sql.Char(1), array[8])
                    .query(`INSERT INTO ${config.table} (FECHA,HORA,[RECIBO],[CUENTA],[CLIENTEID],[VALORP],[TIPO],[reversed])
                    VALUES (CAST(GETDATE() AS date),cast(getdate() as time),@input_3,@input_1,@input_2,@input_4,@input_5,@input_6)`)
            })
            .then((result) => {
                writeToFile(
                    `Operation returned with ${JSON.stringify(
                        result.rowsAffected
                    )} rows affected.`,
                    ext
                )
            })
            .catch((err) => {
                log(err)
            })
    } else if (array[0] == 'CancelPayment') {
        sql.connect(config)
            .then((pool) => {
                return pool
                    .request()
                    .input('input_1', sql.Char, array[3])
                    .input('input_2', sql.Char, array[4])
                    .input('input_3', sql.Char, array[5])
                    .input('input_4', sql.Money, array[6])
                    .input('input_5', sql.Char(1), array[7])
                    .input('input_6', sql.Char(1), array[8])
                    .query(`UPDATE ${config.table}
                    SET [TIPO]=@input_5,[reversed]=@input_6
                    WHERE [RECIBO] = @input_3 AND [CUENTA] = @input_1 AND [CLIENTEID] = @input_2 AND [VALORP] = @input_4 AND [TIPO]='P' AND [reversed]='N'`)
            })
            .then((result) => {
                writeToFile(
                    `Operation updated ${JSON.stringify(
                        result.rowsAffected
                    )} row.`,
                    ext
                )
            })
            .catch((err) => {
                writeToFile(JSON.stringify(err), ext)
            })
    }
}

function buildConfig(array) {
    return {
        user: array[1],
        password: array[2],
        server: process.env.DB_HOST,
        database: process.env.DB_NAME,
        pool: {
            max: 10,
            min: 0,
            idleTimeoutMillis: 750,
        },
        table:
            array[0] == 'GetAccount'
                ? process.env.DB_TABLE1
                : process.env.DB_TABLE2,
    }
}

function writeToFile(content, ext) {
    writeFile(`${process.env.OUTPUT_DIRECTORY}/${ext}`, content, (err) => {
        if (err) {
            console.error(err)
        }
    })
}

async function checkFor(path) {
    try {
        await access(path, constants.R_OK | constants.W_OK)
    } catch (err) {
        createDirectory(path)
    }
}

async function createDirectory(path) {
    mkdir(path, { recursive: true }, function (err) {
        if (err) log(err)
        else log(`Directory created at ${path}`)
    })
}

async function removeFile(path) {
    try {
        await unlink(path)
    } catch (error) {
        log(error.message)
    }
}
