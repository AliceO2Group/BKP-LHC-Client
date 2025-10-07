# BKP-LHC Client

Initial Repository based on work from @iclegrand in repository: https://github.com/iclegrand/AliDip2BK 

The BKP-LHC Client is a java based application which uses the CERN DIP `jar` dependency to consume events from desired tracks. These events are then either:
- published on O2 Kafka Topics to be consumed further by O2 applications (e.g. ECS)
- updates the O2 Bookkeeping application via their HTTP endpoints.

A detailed description for this project is provided by Roberto in this document:
https://codimd.web.cern.ch/G0TSXqA1R8iPqWw2w2wuew
 
### Requirements
- java 11 on a 64 bit system (this is a constrain from the DIP library)
- to test the java version run 
```
java -version 
```

### Configuration
The run configuration is defined in the `AliDip2BK.properties` file.

### Published Events
Currently the BKP-LHC-Client publishes on Kafka (topic: "dip.lhc.beam_mode") events for the start and end of stable beams in the format of `Ev_BeamModeEvent`. The proto file's source of truth is within the [Control Repository](https://github.com/AliceO2Group/Control/blob/master/common/protos/events.proto)

### Bookkeeping Updates
- TBC
