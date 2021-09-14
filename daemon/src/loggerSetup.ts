import { getStorageDir } from "storageDir"
import winston, { format } from "winston"

const storageDir = getStorageDir()

const { printf } = format;
const myFormat = printf(({ level, message }) => {
    return `${(new Date()).toISOString()} ${level}: ${message}`
});
const colorForLevel: {[key: string]: string} = {
    debug: "\x1b[35m",
    info: "\x1b[36m",
    warn: "\x1b[33m",
    error: "\x1b[31m"
}
const myConsoleFormat = printf(({ level, message }: {level: string, message: string}) => {
    return `${colorForLevel[level] || ''}${(new Date()).toISOString()} ${level}: ${message}\x1b[0m`
})

const consoleLogLevel = process.env.CONSOLE_LOG_LEVEL || 'warn'

winston.add(new winston.transports.File({
    filename: `${storageDir}/error.log`,
    level: 'error',
    format: myFormat
}))
winston.add(new winston.transports.File({
    filename: `${storageDir}/warn.log`,
    level: 'warn',
    format: myFormat
}))
winston.add(new winston.transports.File({
    filename: `${storageDir}/info.log`,
    level: 'info',
    format: myFormat
}))
winston.add(new winston.transports.File({
    filename: `${storageDir}/debug.log`,
    level: 'debug',
    format: myFormat
}))
winston.add(
    new winston.transports.Console({
        level: consoleLogLevel,
        format: myConsoleFormat
    })
)

// const logger = winston.createLogger({
//     level: 'debug',
//     // format: winston.format.json(),
//     format: myFormat,
//     // defaultMeta: { service: 'user-service' },
//     transports: [
//         //
//         // - Write all logs with level `error` and below to `error.log`
//         // - Write all logs with level `info` and below to `combined.log`
//         //
//         ,
//         new winston.transports.File({ filename: `${storageDir}/warn.log`, level: 'warn' }),
//         new winston.transports.File({ filename: `${storageDir}/info.log`, level: 'info' }),
//         new winston.transports.File({ filename: `${storageDir}/debug.log`, level: 'debug' }),
//         new winston.transports.Console({level: consoleLogLevel, format: myConsoleFormat})
//     ],
// })

export default winston