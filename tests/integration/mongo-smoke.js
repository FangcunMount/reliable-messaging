// Executed only inside this invocation's disposable replica set container.
const result = rs.initiate({_id: "rm-test", members: [{_id: 0, host: "mongo:27017"}]});
if (result.ok !== 1) throw new Error("replica set initiation failed");
let primary = false;
for (let attempt = 0; attempt < 60; attempt++) {
  if (db.hello().isWritablePrimary) { primary = true; break; }
  sleep(500);
}
if (!primary) throw new Error("replica set did not become primary");
const target = db.getSiblingDB("rm_contract_test");
target.createCollection("business");
target.createCollection("outbox");
const session = db.getMongo().startSession();
try {
  const tx = session.getDatabase("rm_contract_test");
  session.startTransaction();
  tx.business.insertOne({_id: "committed"});
  tx.outbox.insertOne({_id: "committed", payload: "unchanged"});
  session.commitTransaction();
  session.startTransaction();
  tx.business.insertOne({_id: "rolled-back"});
  tx.outbox.insertOne({_id: "rolled-back", payload: "unchanged"});
  session.abortTransaction();
  for (const collection of [target.business, target.outbox]) {
    if (collection.countDocuments({_id: "committed"}) !== 1 ||
        collection.countDocuments({_id: "rolled-back"}) !== 0) {
      throw new Error("transaction probe failed");
    }
  }
  print("PASS Mongo " + db.version() + " replica-set commit/rollback");
} finally {
  session.endSession();
}
