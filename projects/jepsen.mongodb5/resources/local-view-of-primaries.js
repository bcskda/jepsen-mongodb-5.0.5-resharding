const rsStatus = db.adminCommand( { replSetGetStatus : 1 } );
const primaries = rsStatus.members.filter((m) => m.stateStr === "PRIMARY");
const primaryHostnames = primaries.map((m) => m.name.split(":")[0]);
print(primaryHostnames.join(" "))
