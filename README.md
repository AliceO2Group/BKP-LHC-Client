# BKP-LHC-Client

Repository based on work from @iclegrand in repository: https://github.com/iclegrand/AliDip2BK 

Projects consumes selected messages from the CERN DIP system (LHC &amp; ALICE -DCS) and publishes them into the O2 systems. A detailed description for this project is provided by Roberto in this document:
https://codimd.web.cern.ch/G0TSXqA1R8iPqWw2w2wuew
 
### Requirements
- This program requires java 11 on a 64 bit system (this is a constrain from the DIP library)
- maven

### Maven Commands for dev,tst,deployments
```bash
mvn <clean> compile -Dos.version={os_version}
mvn <clean> package -Dos.version={os_version}
```

E.g. os_version `macosx-x86_64`
