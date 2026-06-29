from confluent_kafka import Producer
import json
import time
import random
from datetime import datetime, timezone

TICK_INTERVAL_SECONDS = 5

producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'enable.idempotence': True, # ensure no duplicate messages
    'acks': 'all'
})

def delivery_report(err, msg):
    if err is not None:
        print(f' Delivery failed for {msg.key().decose("utf-8")}: {err}')
        
class Vehicle:
    def __init__(self, vehicle_id):
        self.vehicle_id = vehicle_id
        self.speed = random.uniform(40, 80)
        self.battery = random.uniform(70, 100)
        self.motor_temp = random.uniform(35, 55)
        self.lat = 41.8781 + random.uniform(-0.05, 0.05)
        self.lon = -87.6298 + random.uniform(-0.05, 0.05)
        self.charge_state = 'discharging'
        self.charging = False
        self.odometer_km = 0.0
        self.cumulative_charge_pct = 0.0


    def update(self):
        prev_battery = self.battery

        # Start charging once battery is low, stop once nearly full
        if not self.charging and self.battery < 25 and random.random() < 0.1:
            self.charging = True

        if self.charging:
            self.speed = 0
            self.battery = min(100, self.battery + random.uniform(0.5, 1.5))
            if self.battery >= 99:
                self.charging = False
        else:
            self.speed = max(0, self.speed + random.uniform(-3, 3))
            self.battery = max(0, min(100, self.battery - random.uniform(0.01, 0.08)))

        self.motor_temp = max(20, self.motor_temp + random.uniform(-1.5, 1.5))
        self.lat += random.uniform(-0.001, 0.001)
        self.lon += random.uniform(-0.001, 0.001)

        if not self.charging and random.random() < 0.25:
            spike_type = random.choice(['speed', 'motor_temp', 'battery_drain'])

            if spike_type == 'speed':
                self.speed = random.uniform(205, 250)

            elif spike_type == 'motor_temp':
                self.motor_temp = random.uniform(88, 110)

            elif spike_type == 'battery_drain':
                self.battery = max(0, self.battery - random.uniform(6, 15))

        if self.charging:
            self.charge_state = 'charging'
        elif self.battery < 20:
            self.charge_state = 'idle'
        else:
            self.charge_state = 'discharging'

        # Odometer: distance covered this tick, based on current speed and elapsed time
        self.odometer_km += self.speed * (TICK_INTERVAL_SECONDS / 3600)

        # Cumulative charge throughput: only counts increases in battery
        battery_gain = self.battery - prev_battery
        if battery_gain > 0:
            self.cumulative_charge_pct += battery_gain

    def generate_telemetry(self):
        self.update()
        return {
            'vehicle_id': self.vehicle_id,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'speed_kmh': round(self.speed, 1),
            'battery_pct': round(self.battery, 1),
            'motor_temp_c': round(self.motor_temp, 1),
            'lat': round(self.lat, 6),
            'lon': round(self.lon, 6),
            'charge_state': self.charge_state,
            'odometer_km': round(self.odometer_km, 4),
            'cumulative_charge_pct': round(self.cumulative_charge_pct, 2)
        }
    
if __name__ == '__main__':
    topic = 'fleet-telemetry-raw'
    vehicles = [Vehicle(f'V{i+1:03d}') for i in range(10)]
    
    print(f'Starting fleet simulator = {len(vehicles)} vehicles')
    print(f'Producing to topic: {topic}')
    print(f'Press Ctrl+C to stop\n')
    
    message_count = 0
    
    try:
        while True:
            for vehicle in vehicles:
                telemetry = vehicle.generate_telemetry()
                
                producer.produce(
                    topic,
                    key=telemetry['vehicle_id'],
                    value=json.dumps(telemetry),
                    callback=delivery_report
                )
                
                message_count += 1
            
            producer.poll(0)
            
            print(f'[{datetime.now().strftime("%H:%M:%S")}] '
                  f'Produced {message_count} total messages '
                  f'({len(vehicles)} every {TICK_INTERVAL_SECONDS}s)')

            time.sleep(TICK_INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        print(f'\nStopping simulator. Total messages produced: {message_count}')
        
    finally:
        producer.flush()
        print("Producer flushed and closed.")