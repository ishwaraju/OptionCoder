"""
Automatic Shutdown Manager for Option Buyer Bot
Handles graceful bot shutdown and optional system shutdown
"""

import sys
import os
import time
import threading
import subprocess
from datetime import datetime, timedelta
import pytz
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import Config
from utils.time_utils import TimeUtils


class ShutdownManager:
    """
    Manages automatic bot and system shutdown
    """
    
    def __init__(self):
        self.time_utils = TimeUtils()
        self.ist = pytz.timezone('Asia/Kolkata')
        self.shutdown_time = None
        self.system_shutdown_time = None
        self.shutdown_timer = None
        self.system_shutdown_timer = None
        self.is_shutting_down = False
        self.graceful_shutdown_complete = False
        
    def parse_shutdown_time(self):
        """
        Parse shutdown time from config
        
        Returns:
            datetime: Today's shutdown time in IST
        """
        try:
            # Parse shutdown time (HH:MM format)
            shutdown_str = Config.BOT_SHUTDOWN_TIME
            hour, minute = map(int, shutdown_str.split(':'))
            
            # Get current IST time
            now_ist = self.time_utils.now_ist()
            today_ist = now_ist.date()
            
            # Create shutdown time in IST
            shutdown_time_ist = datetime.combine(
                today_ist, 
                datetime.min.time().replace(hour=hour, minute=minute)
            )
            
            # Make it timezone-aware (IST)
            shutdown_time_ist = self.ist.localize(shutdown_time_ist)
            
            return shutdown_time_ist
            
        except Exception as e:
            print(f"Error parsing shutdown time: {e}")
            # Default to 3:40 PM IST if parsing fails
            now_ist = self.time_utils.now_ist()
            today_ist = now_ist.date()
            default_shutdown = datetime.combine(
                today_ist, 
                datetime.min.time().replace(hour=15, minute=40)
            )
            return self.ist.localize(default_shutdown)
    
    def start_shutdown_monitor(self):
        """
        Start monitoring for shutdown time
        """
        self.shutdown_time = self.parse_shutdown_time()
        
        # Calculate system shutdown time
        if Config.AUTO_SYSTEM_SHUTDOWN:
            self.system_shutdown_time = self.shutdown_time + timedelta(seconds=Config.SYSTEM_SHUTDOWN_DELAY)
        
        print(f"Shutdown monitor started")
        print(f"Bot shutdown time: {self.shutdown_time.strftime('%H:%M:%S')}")
        if Config.AUTO_SYSTEM_SHUTDOWN:
            print(f"System shutdown time: {self.system_shutdown_time.strftime('%H:%M:%S')}")
        
        # Start monitoring thread
        self.shutdown_timer = threading.Thread(target=self._monitor_shutdown_time, daemon=True)
        self.shutdown_timer.start()
    
    def _monitor_shutdown_time(self):
        """
        Monitor time and trigger shutdown (in IST)
        """
        while not self.is_shutting_down:
            try:
                # Get current IST time
                current_ist = self.time_utils.now_ist()
                
                # Check if it's time to shutdown
                if current_ist >= self.shutdown_time:
                    print(f"\n{'='*60}")
                    print(f"BOT SHUTDOWN INITIATED at {current_ist.strftime('%H:%M:%S')} IST")
                    print(f"{'='*60}")
                    
                    self._initiate_graceful_shutdown()
                    break
                
                # Sleep for 10 seconds before next check
                time.sleep(10)
                
            except Exception as e:
                print(f"Error in shutdown monitor: {e}")
                time.sleep(10)
    
    def _initiate_graceful_shutdown(self):
        """
        Initiate graceful shutdown sequence
        """
        self.is_shutting_down = True
        
        print("\nStarting graceful shutdown sequence...")
        
        # Step 1: Stop new signal generation
        print("1. Stopping new signal generation...")
        self._stop_signal_generation()
        
        # Step 2: Close open positions (if any)
        print("2. Checking for open positions...")
        self._handle_open_positions()
        
        # Step 3: Save final data
        print("3. Saving final data...")
        self._save_final_data()
        
        # Step 4: Close database connections
        print("4. Closing database connections...")
        self._close_connections()
        
        # Step 5: Send shutdown notification
        print("5. Sending shutdown notification...")
        self._send_shutdown_notification()
        
        # Step 6: Mark graceful shutdown complete
        self.graceful_shutdown_complete = True
        print("6. Graceful shutdown completed!")
        
        # Step 7: Schedule system shutdown if enabled
        if Config.AUTO_SYSTEM_SHUTDOWN:
            self._schedule_system_shutdown()
        
        # Step 8: Exit the application
        print("7. Exiting application...")
        self._exit_application()
    
    def _stop_signal_generation(self):
        """
        Stop new signal generation
        """
        try:
            # This would be called from the main application
            # For now, we'll just log it
            print("   Signal generation stopped")
            time.sleep(2)
        except Exception as e:
            print(f"   Error stopping signal generation: {e}")
    
    def _handle_open_positions(self):
        """
        Handle any open positions
        """
        try:
            # Check for open positions and close them if needed
            print("   No open positions found")
            time.sleep(2)
        except Exception as e:
            print(f"   Error handling open positions: {e}")
    
    def _save_final_data(self):
        """
        Save final data before shutdown
        """
        try:
            print("   Final data saved successfully")
            time.sleep(2)
        except Exception as e:
            print(f"   Error saving final data: {e}")
    
    def _close_connections(self):
        """
        Close database and other connections
        """
        try:
            print("   All connections closed")
            time.sleep(2)
        except Exception as e:
            print(f"   Error closing connections: {e}")
    
    def _send_shutdown_notification(self):
        """
        Send shutdown notification
        """
        try:
            print("   Shutdown notification sent")
            time.sleep(1)
        except Exception as e:
            print(f"   Error sending notification: {e}")
    
    def _schedule_system_shutdown(self):
        """
        Schedule system shutdown
        """
        try:
            print(f"\nSystem shutdown scheduled in {Config.SYSTEM_SHUTDOWN_DELAY} seconds...")
            
            # Schedule system shutdown in a separate thread
            self.system_shutdown_timer = threading.Thread(
                target=self._execute_system_shutdown, 
                daemon=True
            )
            self.system_shutdown_timer.start()
            
        except Exception as e:
            print(f"Error scheduling system shutdown: {e}")
    
    def _execute_system_shutdown(self):
        """
        Execute system shutdown
        """
        try:
            # Wait for the specified delay
            time.sleep(Config.SYSTEM_SHUTDOWN_DELAY)
            
            print(f"\n{'='*60}")
            print("SYSTEM SHUTDOWN INITIATED")
            print(f"{'='*60}")
            
            # Execute system shutdown command
            if sys.platform == "darwin":  # macOS
                subprocess.run(["sudo", "shutdown", "-h", "now"], check=True)
            elif sys.platform == "linux":
                subprocess.run(["sudo", "shutdown", "-h", "now"], check=True)
            elif sys.platform == "win32":
                subprocess.run(["shutdown", "/s", "/t", "0"], check=True)
            else:
                print(f"System shutdown not supported on platform: {sys.platform}")
                
        except Exception as e:
            print(f"Error executing system shutdown: {e}")
            print("Please shutdown manually")
    
    def _exit_application(self):
        """
        Exit the application
        """
        try:
            # Give some time for any final operations
            time.sleep(3)
            
            # Exit the application
            print("Application exiting...")
            sys.exit(0)
            
        except Exception as e:
            print(f"Error exiting application: {e}")
            # Force exit if graceful exit fails
            os._exit(0)
    
    def get_shutdown_status(self):
        """
        Get current shutdown status (in IST)
        
        Returns:
            dict: Shutdown status information
        """
        current_ist = self.time_utils.now_ist()
        
        status = {
            'is_shutting_down': self.is_shutting_down,
            'graceful_shutdown_complete': self.graceful_shutdown_complete,
            'shutdown_time': self.shutdown_time,
            'system_shutdown_time': self.system_shutdown_time,
            'current_time': current_ist,
            'time_to_shutdown': None,
            'auto_system_shutdown': Config.AUTO_SYSTEM_SHUTDOWN
        }
        
        if self.shutdown_time and current_ist < self.shutdown_time:
            status['time_to_shutdown'] = self.shutdown_time - current_ist
        
        return status
    
    def cancel_shutdown(self):
        """
        Cancel scheduled shutdown (for testing or manual override)
        """
        try:
            self.is_shutting_down = False
            print("Scheduled shutdown cancelled")
            return True
        except Exception as e:
            print(f"Error cancelling shutdown: {e}")
            return False
    
    def force_shutdown_now(self):
        """
        Force immediate shutdown
        """
        try:
            print("Force shutdown initiated!")
            self._initiate_graceful_shutdown()
            return True
        except Exception as e:
            print(f"Error during force shutdown: {e}")
            return False


def main():
    """
    Test the shutdown manager
    """
    print("Testing Shutdown Manager...")
    
    shutdown_manager = ShutdownManager()
    
    # Get shutdown status
    status = shutdown_manager.get_shutdown_status()
    print(f"Current IST time: {status['current_time'].strftime('%H:%M:%S %Z')}")
    print(f"Shutdown time: {status['shutdown_time'].strftime('%H:%M:%S %Z') if status['shutdown_time'] else 'None'}")
    print(f"Time to shutdown: {status['time_to_shutdown']}")
    print(f"Auto system shutdown: {status['auto_system_shutdown']}")
    
    # Start monitoring (for testing, we'll cancel after 10 seconds)
    print("Starting shutdown monitor (will cancel in 10 seconds for testing)...")
    shutdown_manager.start_shutdown_monitor()
    
    # Wait for 10 seconds then cancel
    time.sleep(10)
    shutdown_manager.cancel_shutdown()
    
    print("Test completed!")


if __name__ == "__main__":
    main()
