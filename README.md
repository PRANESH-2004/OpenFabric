# OPENFABRIC
project

#Clone the repository
git clone https://github.com/your-username/transaction-system.git
cd transaction-system

#Install dependencies
pip install httpx
#testing dependencies
pip install locust

#Pull the mock test in docker

#Run the Mock Posting Service (Docker)
docker run -p 8080:8080 vinhopenfabric/mock-posting-service:latest


#Create Redis
docker run -p 6379:6379 redis

#Start Redis
redis-server

#Install python 

#Run the Backend Service
python main.py


