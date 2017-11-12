/**
 * This one-off script calculates totalEnergyMeasured for each Meter,
 * and saves it as totalEnergyMeasured.
 *
 * You can set config param "limit: 5" to limit the number of meters updated (useful for testing).
 */

const config = require('config')
const Promise = require('promise')
const child_process = require('child_process')
const MongoClient = require('mongodb').MongoClient
const connect = Promise.denodeify(MongoClient.connect)

let dbConnection

let totalMeters
let currentMeter = 1

console.log("\n==== AGGREGATE TOTAL ENERGY MEASURED SINCE REPORTING DATE =================================================")
console.log("Connecting to DB...")

connect(config.mongoUrl)

  .then((db) => {
    console.log("Getting meters...");
    dbConnection = db

    const Meters = db.collection('meters')
    return Meters.find({reportingStartDate: {$exists: true}}, {meterName: 1, reportingStartDate: 1})

}).then((metersCursor) => {
  if (config.limit) {
    console.log("Will limit to " + config.limit + " results")
    metersCursor.limit(config.limit)
  }

  return metersCursor.toArray()

}).then((meters) => {
  totalMeters = meters.length
  console.log("Found " + totalMeters + " meters that have reportingStartDate. Going through each one...")

  const calculateEnergyPromises = []

  meters.forEach((meter) => {
    calculateEnergyPromises.push(calculateTotalEnergyMeasuredSinceReportingDate(meter))
  })
  return Promise.all(calculateEnergyPromises)

}).then(() => {
    console.log("\nALL METERS DONE!")
    dbConnection.close()
})
.catch((err) => {
  console.log("Error", err)
})


/**
 * Returns a promise that does the calculation, saves it in the Meter,
 * and logs the result on the console
 */
function calculateTotalEnergyMeasuredSinceReportingDate(meter) {
  let dbConnection
  let totalEnergy
  return connect(config.mongoUrl)
    .then((db) => {
      dbConnection = db
      const EnergyEvents = dbConnection.collection('energy_events')
      const pipeline = [
        {
          $match: {
            meterName: meter.meterName,
            endTime: {'$gte': meter.reportingStartDate}
          }        
        },
        {
          $group: {
            "_id": "totalEnergyForMeterSinceReportingDate" + meter.meterName,
            "totalEnergy": {"$sum": "$energy"}
          }
        }
      ];
      const aggregationCursor = EnergyEvents.aggregate(pipeline)
      return aggregationCursor.next()

    }).then((aggregationResult) => {
      if (aggregationResult) {
        console.assert(aggregationResult.totalEnergy, "No total energy in result! " + JSON.stringify(aggregationResult))
        totalEnergy = Math.floor(aggregationResult.totalEnergy)
      } else {
        totalEnergy = 0
      }
      const Meters = dbConnection.collection('meters')
      return Meters.updateOne(
        {meterName: meter.meterName},
        {$set: {totalEnergyMeasuredSinceReportingDate: totalEnergy}}
      )

    }).then((updateResult) => {
      console.assert(updateResult.modifiedCount == 1, "Failed to update meter " + meter.meterName + ": " + JSON.stringify(updateResult))
      console.log(`  (${currentMeter} / ${totalMeters}) Meter ${meter.meterName} => ${totalEnergy} Wh`)
      currentMeter = currentMeter + 1
      return dbConnection.close()

    }).catch((err) => {
      console.log("Error for meter " + meter.meterName, err)
    })

}


