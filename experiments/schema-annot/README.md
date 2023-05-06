# Explain Compliance

## Commento

----------------------------------------- 
config: UNSHARDED
----------------------------------------- 
owners: DATASUBJECT
----------------------------------------- 
ownerSessions: SHARDED
  ownerHex             shards to owners               (explicit annotation)
      via   ownerSessions(ownerHex) -> owners(ownerHex)
----------------------------------------- 
commenterSessions: SHARDED
  commenterHex         shards to commenters           (explicit annotation)
      via   commenterSessions(commenterHex) -> commenters(commenterHex)
----------------------------------------- 
ownerConfirmHexes: SHARDED
  ownerHex             shards to owners               (explicit annotation)
      via   ownerConfirmHexes(ownerHex) -> owners(ownerHex)
----------------------------------------- 
ownerResetHexes: SHARDED
  ownerHex             shards to owners               (explicit annotation)
      via   ownerResetHexes(ownerHex) -> owners(ownerHex)
----------------------------------------- 
domains: SHARDED
  ownerHex             shards to owners               (explicit annotation)
      via   domains(ownerHex) -> owners(ownerHex)
----------------------------------------- 
moderators: DATASUBJECT AND SHARDED
  domain               shards to owners               (implicitly deduced)
      via   moderators(domain) -> domains(domain)
      via   moderators(ownerHex) -> owners(ownerHex)
    with a total distance of 2
----------------------------------------- 
commenters: DATASUBJECT
----------------------------------------- 
comments: SHARDED
  commenterHex         shards to commenters           (explicit annotation)
      via   comments(commenterHex) -> commenters(commenterHex)
----------------------------------------- 
votes: SHARDED
  commenterHex         shards to commenters           (explicit annotation)
      via   votes(commenterHex) -> commenters(commenterHex)
----------------------------------------- 
views: SHARDED
  commenterHex         shards to commenters           (explicit annotation)
      via   views(commenterHex) -> commenters(commenterHex)