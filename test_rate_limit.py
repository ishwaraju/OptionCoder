#!/usr/bin/env python3
"""
Rate Limit Test - Check if 429 errors are occurring
Tests Dhan API endpoints for rate limiting
"""

import sys
import os
import time
import requests
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import Config


class RateLimitTester:
    def __init__(self):
        self.base_url = "https://api.dhan.co"
        self.headers = {
            "Content-Type": "application/json",
            "access-token": Config.DHAN_ACCESS_TOKEN,
            "client-id": Config.DHAN_CLIENT_ID
        }
        self.results = []
        
    def log(self, msg, status="INFO"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        icon = {"INFO": "ℹ️", "OK": "✅", "ERROR": "❌", "WARN": "⚠️"}.get(status, "ℹ️")
        print(f"{icon} [{timestamp}] {msg}")
        
    def test_endpoint(self, name, method, url, payload=None, delay=0):
        """Test a single endpoint"""
        time.sleep(delay)
        start = time.time()
        try:
            if method == "GET":
                response = requests.get(url, headers=self.headers, timeout=10)
            else:
                response = requests.post(url, headers=self.headers, json=payload, timeout=10)
            
            elapsed = (time.time() - start) * 1000
            status = response.status_code
            
            if status == 429:
                self.log(f"{name}: ❌ 429 RATE LIMITED ({elapsed:.0f}ms)", "ERROR")
                self.results.append({"endpoint": name, "status": 429, "error": "Rate limited"})
                return False
            elif status == 200:
                self.log(f"{name}: ✅ OK ({elapsed:.0f}ms)", "OK")
                self.results.append({"endpoint": name, "status": 200})
                return True
            else:
                self.log(f"{name}: ⚠️ HTTP {status} ({elapsed:.0f}ms)", "WARN")
                self.results.append({"endpoint": name, "status": status})
                return False
                
        except Exception as e:
            self.log(f"{name}: ❌ Error - {str(e)[:50]}", "ERROR")
            self.results.append({"endpoint": name, "status": "ERROR", "error": str(e)})
            return False
    
    def test_expiry_list(self):
        """Test option chain expiry list endpoint"""
        url = f"{self.base_url}/v2/optionchain/expirylist"
        # CORRECT: Use integer security ID (13 for NIFTY) and IDX_I segment
        payload = {
            "UnderlyingScrip": 13,
            "UnderlyingSeg": "IDX_I"
        }
        return self.test_endpoint("Expiry List (NIFTY)", "POST", url, payload)
    
    def test_option_chain(self):
        """Test option chain endpoint with valid expiry"""
        url = f"{self.base_url}/v2/optionchain"
        # First get valid expiry, then use it
        expiry_url = f"{self.base_url}/v2/optionchain/expirylist"
        expiry_payload = {"UnderlyingScrip": 13, "UnderlyingSeg": "IDX_I"}
        
        try:
            resp = requests.post(expiry_url, headers=self.headers, json=expiry_payload, timeout=10)
            if resp.status_code == 200:
                expiry = resp.json().get("data", ["2026-04-13"])[0]
            else:
                expiry = "2026-04-13"
        except:
            expiry = "2026-04-13"
        
        payload = {
            "UnderlyingScrip": 13,
            "UnderlyingSeg": "IDX_I",
            "Expiry": expiry
        }
        return self.test_endpoint(f"Option Chain (NIFTY) [{expiry}]", "POST", url, payload)
    
    def test_expiry_list_banknifty(self):
        """Test option chain expiry list for BANKNIFTY"""
        url = f"{self.base_url}/v2/optionchain/expirylist"
        payload = {
            "UnderlyingScrip": 25,  # BANKNIFTY security ID
            "UnderlyingSeg": "IDX_I"
        }
        return self.test_endpoint("Expiry List (BANKNIFTY)", "POST", url, payload)
    
    def test_expiry_list_sensex(self):
        """Test option chain expiry list for SENSEX"""
        url = f"{self.base_url}/v2/optionchain/expirylist"
        payload = {
            "UnderlyingScrip": 51,  # SENSEX security ID
            "UnderlyingSeg": "IDX_I"
        }
        return self.test_endpoint("Expiry List (SENSEX)", "POST", url, payload)
    
    def test_option_chain_banknifty(self):
        """Test option chain for BANKNIFTY with correct expiry"""
        # First get valid expiry for BANKNIFTY
        expiry_url = f"{self.base_url}/v2/optionchain/expirylist"
        expiry_payload = {"UnderlyingScrip": 25, "UnderlyingSeg": "IDX_I"}
        
        try:
            resp = requests.post(expiry_url, headers=self.headers, json=expiry_payload, timeout=10)
            if resp.status_code == 200:
                expiry = resp.json().get("data", ["2026-04-13"])[0]
            else:
                expiry = "2026-04-13"
        except:
            expiry = "2026-04-13"
        
        url = f"{self.base_url}/v2/optionchain"
        payload = {
            "UnderlyingScrip": 25,
            "UnderlyingSeg": "IDX_I",
            "Expiry": expiry
        }
        return self.test_endpoint(f"Option Chain (BANKNIFTY) [{expiry}]", "POST", url, payload)
    
    def test_rapid_requests(self, count=5, delay=0.5):
        """Test rapid sequential requests to trigger rate limit"""
        self.log(f"Testing {count} rapid requests with {delay}s delay...")
        
        url = f"{self.base_url}/v2/optionchain/expirylist"
        # CORRECT: Use integer security ID
        payload = {
            "UnderlyingScrip": 13,
            "UnderlyingSeg": "IDX_I"
        }
        
        errors_429 = 0
        for i in range(count):
            success = self.test_endpoint(f"Rapid Req {i+1}/{count}", "POST", url, payload, delay)
            if not success:
                errors_429 += 1
        
        return errors_429
    
    def test_burst_requests(self, count=10):
        """Test burst requests with minimal delay"""
        self.log(f"Testing {count} burst requests (0.1s delay)...")
        return self.test_rapid_requests(count, delay=0.1)
    
    def run_all_tests(self):
        """Run complete test suite"""
        print("=" * 60)
        print("🧪 Dhan API Rate Limit Test Suite")
        print("=" * 60)
        print(f"Client ID: {Config.DHAN_CLIENT_ID[:10]}...")
        print(f"Base URL: {self.base_url}")
        print("=" * 60)
        
        # Basic endpoint tests
        self.log("\n--- Basic Endpoint Tests ---")
        self.test_expiry_list()
        time.sleep(1)
        self.test_expiry_list_banknifty()
        time.sleep(1)
        self.test_expiry_list_sensex()
        time.sleep(1)
        self.test_option_chain()
        time.sleep(1)
        self.test_option_chain_banknifty()
        
        # Rate limit stress tests
        self.log("\n--- Rate Limit Stress Tests ---")
        errors = self.test_rapid_requests(count=5, delay=0.5)
        
        if errors == 0:
            self.log("\n--- Burst Test (Aggressive) ---")
            errors = self.test_burst_requests(count=10)
        
        # Summary
        print("\n" + "=" * 60)
        print("📊 TEST SUMMARY")
        print("=" * 60)
        
        total = len(self.results)
        ok = sum(1 for r in self.results if r["status"] == 200)
        rate_limited = sum(1 for r in self.results if r["status"] == 429)
        errors_other = total - ok - rate_limited
        
        print(f"Total Requests: {total}")
        print(f"✅ Successful: {ok}")
        print(f"❌ Rate Limited (429): {rate_limited}")
        print(f"⚠️  Other Errors: {errors_other}")
        
        if rate_limited > 0:
            print("\n⚠️  RATE LIMITING DETECTED!")
            print("   Recommendation: Add delays between API calls")
        else:
            print("\n✅ No rate limiting issues detected")
        
        print("=" * 60)
        return rate_limited == 0


    def test_market_quote_api(self):
        """Test Market Quote API for futures volume"""
        import json
        
        self.log("\n--- Market Quote API Test ---")
        
        # Load future ID
        future_ids_file = os.path.join(os.path.dirname(__file__), "data", "future_ids.json")
        if os.path.exists(future_ids_file):
            with open(future_ids_file) as f:
                future_ids = json.load(f)
            nifty_fut_id = future_ids.get("NIFTY", 66688)
        else:
            nifty_fut_id = 66688
        
        # Try using dhanhq library method
        try:
            from dhanhq import dhanhq
            dhan = dhanhq(Config.DHAN_CLIENT_ID, Config.DHAN_ACCESS_TOKEN)
            
            securities = {"NSE_FNO": [nifty_fut_id]}
            result = dhan.quote_data(securities)
            
            if result.get("status") == "success":
                # Navigate to correct path: data.data.NSE_FNO.{security_id}.volume
                data = result.get("data", {}).get("data", {})
                fno_data = data.get("NSE_FNO", {})
                instrument = fno_data.get(str(nifty_fut_id), {})
                volume = instrument.get("volume", 0)
                ltp = instrument.get("last_price", 0)
                
                if volume:
                    self.log(f"NIFTY FUT Volume from API: {volume:,} | LTP: {ltp}", "OK")
                    return True
                else:
                    self.log(f"No volume in API response. LTP: {ltp}", "WARN")
            else:
                self.log(f"API status: {result.get('status')}", "WARN")
        except Exception as e:
            self.log(f"API Error: {str(e)[:80]}", "ERROR")
        
        return False

    def test_websocket_modes(self):
        """Test WebSocket feed modes and request codes"""
        import websocket
        import json
        import ssl
        
        self.log("\n--- WebSocket Feed Mode Tests ---")
        
        ws_url = f"wss://api-feed.dhan.co?version=2&token={Config.DHAN_ACCESS_TOKEN}&clientId={Config.DHAN_CLIENT_ID}&authType=2"
        
        results = {
            "mode_8_full": {"works": False, "has_volume": False, "data": None},
        }
        
        # Load correct future ID from cache file
        import json
        future_ids_file = os.path.join(os.path.dirname(__file__), "data", "future_ids.json")
        if os.path.exists(future_ids_file):
            with open(future_ids_file) as f:
                future_ids = json.load(f)
            nifty_fut_id = future_ids.get("NIFTY", 66688)
        else:
            nifty_fut_id = getattr(Config, "NIFTY_FUTURE_ID", 66688) or 66688
        
        nifty_instrument = [{"ExchangeSegment": "NSE_FNO", "SecurityId": str(nifty_fut_id)}]
        self.log(f"Using NIFTY FUT Security ID: {nifty_fut_id}")
        
        def test_mode(request_code, mode_name, result_key):
            self.log(f"Testing Request Code {request_code} ({mode_name})...")
            received_msgs = []
            
            def on_message(ws, message):
                if isinstance(message, bytes):
                    received_msgs.append(message)
                    self.log(f"  Received: {len(message)} bytes, code={message[0] if message else 'N/A'}")
                else:
                    self.log(f"  Text: {message[:80]}")
            
            def on_open(ws):
                # Test both index in ticker + futures in quote (like real live_feed)
                index_inst = [{"ExchangeSegment": "IDX_I", "SecurityId": "13"}]  # NIFTY index
                
                # Subscribe index in mode 15
                ws.send(json.dumps({
                    "RequestCode": 15,
                    "InstrumentCount": 1,
                    "InstrumentList": index_inst,
                }))
                self.log(f"  Subscribed INDEX in mode 15")
                
                # Subscribe futures in requested mode
                ws.send(json.dumps({
                    "RequestCode": request_code,
                    "InstrumentCount": 1,
                    "InstrumentList": nifty_instrument
                }))
                self.log(f"  Subscribed FUTURES in mode {request_code}")
            
            def on_error(ws, error):
                self.log(f"  WebSocket Error: {error}", "ERROR")
            
            def on_close(ws, code, msg):
                self.log(f"  WebSocket Closed: {code} - {msg}")
            
            try:
                ws = websocket.WebSocketApp(
                    ws_url,
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                )
                
                # Run for 8 seconds (longer for quote mode)
                import threading
                ws_thread = threading.Thread(target=lambda: ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}))
                ws_thread.daemon = True
                ws_thread.start()
                
                time.sleep(8 if request_code == 4 else 5)  # Longer wait for quote
                ws.close()
                ws_thread.join(timeout=2)
                
                # Parse messages
                from shared.feeds.binary_parser import BinaryParser
                parser = BinaryParser()
                
                self.log(f"  Total msgs: {len(received_msgs)}")
                
                for msg in received_msgs:
                    data = parser.parse_packet(msg)
                    if data and data.get("security_id"):
                        results[result_key]["works"] = True
                        results[result_key]["data"] = data
                        self.log(f"  Parsed: type={data.get('type')}, vol={data.get('volume')}")
                        if data.get("volume") is not None and data.get("volume") > 0:
                            results[result_key]["has_volume"] = True
                            self.log(f"  VOLUME FOUND: {data.get('volume')}", "OK")
                        break
                
                if results[result_key]["works"]:
                    vol_status = "WITH VOLUME" if results[result_key]["has_volume"] else "NO VOLUME"
                    status = "OK" if results[result_key]["has_volume"] else "WARN"
                    self.log(f"Mode {request_code}: {vol_status}", status)
                else:
                    self.log(f"Mode {request_code}: No data", "WARN")
                    
            except Exception as e:
                self.log(f"Mode {request_code}: Error - {str(e)[:80]}", "ERROR")
        
        # Test only mode 8
        test_mode(8, "Full", "mode_8_full")
        
        # Summary
        self.log("\n--- WebSocket Mode Summary ---")
        self.log(f"Request Code 8 (Full): {'Volume Available' if results['mode_8_full']['has_volume'] else 'NO Volume'}")
        
        # Recommendation
        if results["mode_8_full"]["has_volume"]:
            self.log("\n For NIFTY FUT volume - Use Request Code 8 (Full mode)", "OK")
        else:
            self.log("\n Mode 8 not providing volume - Use API fallback", "WARN")
        
        return results

    def run_all_tests(self):
        """Run complete test suite"""
        print("=" * 60)
        print(" Dhan API Rate Limit Test Suite")
        print("=" * 60)
        print(f"Client ID: {Config.DHAN_CLIENT_ID[:10]}...")
        print(f"Base URL: {self.base_url}")
        print("=" * 60)
        
        # Market Quote API test for futures volume
        self.test_market_quote_api()
        
        # WebSocket mode test
        self.test_websocket_modes()
        
        # Basic endpoint tests
        self.log("\n--- Basic Endpoint Tests ---")
        self.test_expiry_list()
        time.sleep(1)
        self.test_expiry_list_banknifty()
        time.sleep(1)
        self.test_expiry_list_sensex()
        time.sleep(1)
        self.test_option_chain()
        time.sleep(1)
        self.test_option_chain_banknifty()
        
        # Rate limit stress tests
        self.log("\n--- Rate Limit Stress Tests ---")
        errors = self.test_rapid_requests(count=5, delay=0.5)
        
        if errors == 0:
            self.log("\n--- Burst Test (Aggressive) ---")
            errors = self.test_burst_requests(count=10)
        
        # Summary
        print("\n" + "=" * 60)
        print(" TEST SUMMARY")
        print("=" * 60)
        
        total = len(self.results)
        ok = sum(1 for r in self.results if r["status"] == 200)
        rate_limited = sum(1 for r in self.results if r["status"] == 429)
        errors_other = total - ok - rate_limited
        
        print(f"Total Requests: {total}")
        print(f"Successful: {ok}")
        print(f"Rate Limited (429): {rate_limited}")
        print(f"Other Errors: {errors_other}")
        
        if rate_limited > 0:
            print("\n RATE LIMITING DETECTED!")
            print("   Recommendation: Add delays between API calls")
        else:
            print("\n No rate limiting issues detected")
        
        print("=" * 60)
        return rate_limited == 0


def main():
    tester = RateLimitTester()
    success = tester.run_all_tests()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
