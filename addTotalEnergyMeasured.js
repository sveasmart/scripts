/**
 * This one-off script calculates totalEnergyMeasured for each Meter,
 * and saves it as totalEnergyMeasured.
 */

const config = require('config')
const Promise = require('promise')
const child_process = require('child_process')
const MongoClient = require('mongodb').MongoClient
const connect = Promise.denodeify(MongoClient.connect)

let dbConnection

let totalMeters
let currentMeter = 1

console.log("\n====================================================================")
console.log("Connecting to DB...")

connect(config.mongoUrl)

  .then((db) => {
    console.log("Getting meters...");
    dbConnection = db

    const Meters = db.collection('meters')
    return Meters.find({totalEnergyMeasured: {$exists: false}}, {meterName: 1})

}).then((metersCursor) => {
  if (config.limit) {
    console.log("Will limit to " + config.limit + " results")
    metersCursor.limit(config.limit)
  }

  return metersCursor.toArray()

}).then((meters) => {
  totalMeters = meters.length
  console.log("Found " + totalMeters + " meters with no totalEnergyMeasured. Going through each one...")

  const calculateEnergyPromises = []

  meters.forEach((meter) => {
    calculateEnergyPromises.push(calculateTotalEnergyMeasured(meter.meterName))
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
function calculateTotalEnergyMeasured(meterName) {
  let dbConnection
  let totalEnergy
  return connect(config.mongoUrl)
    .then((db) => {
      dbConnection = db
      const EnergyEvents = dbConnection.collection('energy_events')
      const pipeline = [
        {
          $match: {meterName: meterName}
        },
        {
          $group: {
            "_id": "totalEnergyForMeter" + meterName,
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
      return Meters.updateOne({meterName: meterName}, {$set: {totalEnergyMeasured: totalEnergy}})

    }).then((updateResult) => {
      console.assert(updateResult.modifiedCount == 1, "Failed to update meter " + meterName + ": " + JSON.stringify(updateResult))
      console.log(`  (${currentMeter} / ${totalMeters}) Meter ${meterName} => ${totalEnergy} Wh`)
      currentMeter = currentMeter + 1
      return dbConnection.close()

    }).catch((err) => {
      console.log("Error for meter " + meterName, err)
    })

}


