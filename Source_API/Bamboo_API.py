import requests
import csv
from datetime import datetime, timedelta
import sys
import time
import random
import os

# Safe print function for console output
def safe_print(msg):
    try:
        print(msg, flush=True)
    except UnicodeEncodeError:
        ascii_msg = msg.encode('ascii', errors='replace').decode('ascii')
        print(ascii_msg)
    except Exception:
        print("[ERROR] Could not display message")

# Date range configuration
start_date = datetime.strptime("2025-05-28", "%Y-%m-%d")
end_date = datetime.strptime("2026-05-28", "%Y-%m-%d")

# Output CSV file path - Save in current directory
output_path = "bamboo_flight_prices5.csv"

headers = { "accept": "application/json",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "vi-VN,vi;q=0.9,fr-FR;q=0.8,fr;q=0.7,en-US;q=0.6,en;q=0.5",
    "ama-client-facts": "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJzdWIiOiJmYWN0IiwiY291bnRyeUNvZGUiOiJWTiIsInVzZU1pbGVzIjoiZmFsc2UiLCJiYWdnYWdlTWFya2V0IjoiIiwiYm9va2luZ0Zsb3ciOiJ0cnVlIn0.",
    "ama-client-ref": "caa64ebf-a41a-4e51-aad2-bfc33a9e08bb:2",
    "authorization": "Bearer GeG48GNRgmwwDLdnO34r67I81Ikd",
    "cache-control": "max-age=0",
    "content-type": "application/json",
    "origin": "https://digital.bambooairways.com",
    "priority": "u=1, i",
    "referer": "https://digital.bambooairways.com/",
    "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "x-d-token": "3:xFPwp+Nw/GUMwZFgEJEf7g==:xAe0Hk02Lie87JFCV0N3gtrj7Fv4KGcMHRKgzTY9AVYW6rMySiDDpUQlQBSafUc29Er74i6FCf+X6v47Z93S+0OSgm6FUWOudjzPVR1C80p05wlQR6+KrC5IHZu0OPMA6diLZdxtSohI3sAB/1ZkaxCZLqgQ0ySwI1gZ+ls5lPrp+bFeZO1+eHblaC6Ny17zV43yDNNFoDcxp5jSGrm8sOsaSGHdmzXQtF6K4weoWHuF7nSWyQzkLnPmHnrA5y0CirdZOJnbbJmUUbzBZjd2pHW/m+1fTyxNOvR5BnbhCy2wNkdxYDqCrQXsp41Xd49k9NY5Hpjz6hnHlhUP93veDK9X0StXgrK0xDMjLy1bi4nbhdulBhcLgOUeeG1rdduuz/qLqliydQwDnSc4XmNe3N9sED4gbBF/euIo1WTDuP+ikn9YmAPM+1hYCKSxOZfmzQwiHLNxFSuNjwdEbPFXCPkjqs7s6Ba1MwLzn5LxZFR6zmpMHwU1PDE1QmG7ZalEUaT+h/C8oCrkjfWc+ALyCuMGSmcRDhVzYIEv3tfZxJm/GLTt0TRseoPKxecERWnHvayA0J6UahOsI9Wz6EObgkMe7Eb/Jf5oGL3cleGBMmSkG7O4fsuNYGa2BvsCQONxJyGO93Xfr9PROQtO6+HswGFiJ26Z/Ace8SKwNu8PuGhWf/YcvNtp8KKttwOb33QhWuW3yDGHdQyDLnbBNOHeB8NmWx3wDewvj6opiYUEsPvDViB7NPpPL820zmcV4Q8kHNZC4DMBpuonKmlgtpJdwS5YxVwHIkqm6+2YmX2E7JSxnKONHvVx8TqAWP5l//564lW3kem3C43sB7UYwCuhbr2t0XD9fnVCVlX0CUmJBTrxycZHVKVVvM0U1jQHj3Yi:YhBdjOLB+We24d3NuaSQRVdXg/dy4pVgqKHEjUovVE8="
}

def get_fresh_session():
    session = requests.Session()
    session.headers.update(headers)
    return session

def make_request_with_retry(session, payload, max_retries=3):
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                delay = (2 ** attempt) + random.uniform(1, 3)
                safe_print(f"Retrying in {delay:.1f} seconds...")
                time.sleep(delay)

            response = session.post(
                "https://api-des.bambooairways.com/v2/search/air-bounds",
                json=payload,
                timeout=30
            )

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 403:
                safe_print("ERROR 403: Access forbidden. Token may have expired.")
                return None
            elif response.status_code == 429:
                safe_print("ERROR 429: Rate limited. Waiting longer...")
                time.sleep(60)
                continue
            else:
                safe_print(f"ERROR {response.status_code}: {response.text[:200]}")

        except requests.exceptions.RequestException as e:
            safe_print(f"Request error (attempt {attempt + 1}): {e}")

    return None

dates_to_check = []
curr = start_date
while curr <= end_date:
    dates_to_check.append(curr.strftime("%Y-%m-%dT00:00:00.000"))
    curr += timedelta(days=1)

routes = [("HPH","DAD"),("DAD","HPH")]
session = get_fresh_session()

try:
    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Date", "Route", "Flight_ID", "Origin", "Destination", "Fare_Code", "Fare_Class", "Price_VND", "Taxes",
            "Departure_Time", "Arrival_Time", "Booking_Class", "Cabin", "Quota", "Status", "Service_Code", "Fare_Conditions",
            "Operating_Airline_Code", "Operating_Airline_Name"
        ])

        total_requests = len(routes) * len(dates_to_check)
        current_request = 0
        successful_requests = 0
        failed_requests = 0

        for origin, dest in routes:
            for date_str in dates_to_check:
                current_request += 1
                safe_print(f"[{current_request}/{total_requests}] Processing {date_str[:10]} {origin}->{dest}")

                payload = {
                    "commercialFareFamilies": ["QHECO", "QHBUZ"],
                    "itineraries": [{
                        "departureDateTime": date_str,
                        "originLocationCode": origin,
                        "destinationLocationCode": dest,
                        "isRequestedBound": True
                    }],
                    "travelers": [{"passengerTypeCode": "ADT"}],
                    "searchPreferences": {"showMilesPrice": False}
                }

                data = make_request_with_retry(session, payload)

                if data is None:
                    failed_requests += 1
                    continue

                if 'data' not in data or 'airBoundGroups' not in data['data']:
                    continue

                try:
                    for group in data['data']['airBoundGroups']:
                        bound = group['boundDetails']
                        segments = bound.get('segments', [])
                        flight_ids = [seg.get('flightId', '') for seg in segments]
                        airline_code = "QH"
                        airline_name = "BAMBOO AIRWAYS"

                        dep_times = []
                        arr_times = []
                        for seg_id in flight_ids:
                            flight_info = data.get("dictionaries", {}).get("flight", {}).get(seg_id, {})
                            dep_times.append(flight_info.get("departure", {}).get("dateTime", ""))
                            arr_times.append(flight_info.get("arrival", {}).get("dateTime", ""))

                        flight_id_combined = ";".join(flight_ids)
                        dep_time = dep_times[0] if dep_times else ''
                        arr_time = arr_times[-1] if arr_times else ''

                        for offer in group['airBounds']:
                            try:
                                prices = offer.get("prices", {}).get("totalPrices", [])
                                if not prices:
                                    continue
                                total_price = prices[0].get("total", '')
                                total_taxes = prices[0].get("totalTaxes", '')
                                fare_code = offer.get('fareFamilyCode', '')

                                booking_class = ''
                                cabin = ''
                                quota = ''
                                status = ''
                                fare_class = ''
                                service_code = ''
                                fare_conditions = ''

                                if 'availabilityDetails' in offer and offer['availabilityDetails']:
                                    details = offer['availabilityDetails'][0]
                                    booking_class = details.get('bookingClass', '')
                                    cabin = details.get('cabin', '')
                                    quota = details.get('quota', '')
                                    status = details.get('statusCode', '')

                                if 'fareInfos' in offer and offer['fareInfos']:
                                    fare_class = offer['fareInfos'][0].get('fareClass', '')

                                if 'services' in offer and offer['services']:
                                    service_code = offer['services'][0].get('serviceCode', '')

                                if 'fareConditionsCodes' in offer:
                                    fare_conditions = ','.join(offer['fareConditionsCodes'])

                                writer.writerow([
                                    date_str[:10], f"{origin}_{dest}", flight_id_combined, origin, dest, fare_code, fare_class,
                                    total_price, total_taxes, dep_time, arr_time, booking_class, cabin, quota, status,
                                    service_code, fare_conditions, airline_code, airline_name
                                ])

                            except (KeyError, IndexError):
                                continue

                    successful_requests += 1

                except Exception as e:
                    failed_requests += 1

                time.sleep(random.uniform(2, 4))

        safe_print("-" * 60)
        safe_print("SCRAPING COMPLETED!")
        safe_print(f"Total requests: {total_requests}")
        safe_print(f"Successful: {successful_requests}")
        safe_print(f"Failed: {failed_requests}")
        safe_print(f"Success rate: {(successful_requests/total_requests)*100:.1f}%")
        safe_print(f"Data saved to: {output_path}")

except IOError as file_err:
    safe_print(f"ERROR: File operation failed: {file_err}")
except KeyboardInterrupt:
    safe_print("\nScript interrupted by user")
except Exception as general_err:
    safe_print(f"ERROR: General error: {general_err}")

safe_print("Script execution completed.")
