const checkProgress = (ns) => db
    .getSiblingDB("admin")
    .aggregate([
        { $currentOp: { allUsers: true, localOps: false } },
        { $match: { type: "op", "originatingCommand.reshardCollection": ns } }
    ]
);


const ns = "%s";
const newKey = %s;
const command = {reshardCollection: ns, key: newKey};

const result = db.adminCommand(command);
printjson(result);

assert(result.ok === 1);
