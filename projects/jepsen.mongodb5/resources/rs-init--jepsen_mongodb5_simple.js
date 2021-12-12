rs.initiate({
  _id: "jepsen_mongodb5_simple",
  members: [
    {_id: 0, host: "n1"},
    {_id: 1, host: "n2"},
    {_id: 2, host: "n3"},
    {_id: 3, host: "n4"},
    {_id: 4, host: "n5"}
  ]
})
