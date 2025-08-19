#!/usr/bin/env python3
"""
Find the actual port where the mock service is running
"""

import asyncio
import httpx
from concurrent.futures import ThreadPoolExecutor

# Common ports to check
PORTS_TO_CHECK = [8080, 8081, 8082, 8083, 8084, 8085, 8086, 8087, 8088, 8089, 8090, 3000, 5000, 9000]

async def check_port(port: int) -> dict:
    """Check if a service is running on a specific port"""
    try:
        async with httpx.AsyncClient() as client:
            # Try health endpoint first
            try:
                response = await client.get(f"http://localhost:{port}/health", timeout=3.0)
                return {
                    "port": port,
                    "status": "responsive",
                    "endpoint": "/health",
                    "http_status": response.status_code,
                    "headers": dict(response.headers),
                    "content": response.text[:200] + "..." if len(response.text) > 200 else response.text
                }
            except httpx.ConnectError:
                return {"port": port, "status": "connection_refused"}
            except httpx.TimeoutException:
                return {"port": port, "status": "timeout"}
                
    except Exception as e:
        return {"port": port, "status": "error", "error": str(e)}

async def scan_ports():
    """Scan multiple ports concurrently"""
    print("Scanning for services on common ports...")
    print("=" * 60)
    
    # Check all ports concurrently
    tasks = [check_port(port) for port in PORTS_TO_CHECK]
    results = await asyncio.gather(*tasks)
    
    # Filter and display results
    active_services = []
    for result in results:
        if result["status"] == "responsive":
            active_services.append(result)
            port = result["port"]
            status = result["http_status"]
            
            print(f"\n🌐 Port {port}: HTTP {status}")
            print("-" * 30)
            
            # Check if it looks like our mock service
            headers = result["headers"]
            content = result["content"]
            
            # Look for Go/Gin indicators (our mock service)
            if "gin" in content.lower() or "go" in headers.get("server", "").lower():
                print("✅ This might be our Go mock service!")
            elif "jenkins" in content.lower() or "jenkins" in str(headers).lower():
                print("❌ This is Jenkins")
            elif "nginx" in str(headers).lower():
                print("ℹ️  This is Nginx")
            elif "apache" in str(headers).lower():
                print("ℹ️  This is Apache")
            else:
                print("❓ Unknown service type")
            
            print(f"Server: {headers.get('server', 'Unknown')}")
            print(f"Content preview: {content[:100]}...")
    
    if not active_services:
        print("❌ No responsive services found on common ports")
        print("\nTry checking:")
        print("1. Is your mock service actually running?")
        print("2. Check the mock service logs for the actual port")
        print("3. Try: netstat -an | findstr LISTENING")
    else:
        print(f"\n📊 Found {len(active_services)} active services")
        
        # Look for likely mock service candidates
        candidates = []
        for service in active_services:
            headers = str(service["headers"]).lower()
            content = service["content"].lower()
            
            # Skip obvious non-candidates
            if any(keyword in content or keyword in headers for keyword in ["jenkins", "apache", "nginx", "iis"]):
                continue
                
            # Look for mock service indicators
            if any(keyword in content for keyword in ["transaction", "mock", "gin", "go"]):
                candidates.append(service["port"])
            elif service["http_status"] == 404:  # Might be our service with no root endpoint
                candidates.append(service["port"])
        
        if candidates:
            print(f"\n🎯 Likely mock service candidates: {candidates}")
            print("Update your MOCK_SERVICE_URL to use one of these ports")

if __name__ == "__main__":
    asyncio.run(scan_ports())