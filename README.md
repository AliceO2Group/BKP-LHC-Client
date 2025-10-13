# BKP-LHC-Client

Repository based on work from @iclegrand in repository: https://github.com/iclegrand/AliDip2BK 

The BKP-LHC Client is a java based application which uses the CERN DIP `jar` dependency to consume events from desired tracks. These events are then either:
- published on O2 Kafka Topics to be consumed further by O2 applications (e.g. ECS)
- updates the O2 Bookkeeping application via their HTTP endpoints.

A detailed description for this project is provided by Roberto in this document:
https://codimd.web.cern.ch/G0TSXqA1R8iPqWw2w2wuew
 
### Published Events
Currently the BKP-LHC-Client publishes on Kafka (topic: "dip.lhc.beam_mode") events for the start and end of stable beams in the format of `Ev_BeamModeEvent`. The proto file's source of truth is within the [Control Repository](https://github.com/AliceO2Group/Control/blob/master/common/protos/events.proto)

### Requirements
- This program requires java 11 on a 64 bit system (this is a constrain from the DIP library)
- maven
- 
### Configuration
The run configuration is defined in the `AliDip2BK.properties` file.

### Maven Commands for dev,tst,deployments
```bash
mvn <clean> compile -Dos.version={os_version}
mvn <clean> package -Dos.version={os_version}
mvn tst -Dos.version={os_version}
```

E.g. os_version `macosx-x86_64`
