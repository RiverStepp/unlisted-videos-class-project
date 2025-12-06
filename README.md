## First Time Setup
### Frontend:  
Download Node.js  
cd into frontend folder  
"npm install" to install packages  
"npm run dev" to start the dev server  

### Backend:   
download .NET 8  
cd into backend folder  
"dotnet restore" to install packages  
Install docker desktop & start docker  
Install elasticsearch server from powershell with this command:  
Install & run the elasticsearch server from powershell using this command:  
```powershell
docker run --name es-dev `
  -p 9200:9200 `
  -e "discovery.type=single-node" `
  -e "xpack.security.enabled=false" `
  docker.elastic.co/elasticsearch/elasticsearch:8.15.0
```

After this, clean, rebuild, and run the visual studio solution to start the API  

### Scripts:  
Scripts can be run standalone with no special setup, just make sure you update the connection strings and all that  

## Rerunning  
Navigate to frontend folder and type "npm run dev"    
Open docker desktop  
Once docker desktop is opened open powershell and run "docker start es-dev"  
Clean, rebuild, and run the visual studio solution  

**Contributors:** 
Carver Ossoff
River Stepp
Quynh Tran 
