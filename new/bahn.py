"""
Scraper for Deutsche Bahn parking lots

This scraper is disabled unless you define your DB credentials, either in
- environment: e.g. `export DB_CLIENT_ID=xxx && export DB_API_KEY=yyy` before running the scraper
- or in a `.env` file in the root of the scraper package containing: `DB_CLIENT_ID=xxx DB_API_KEY=yyy`
"""

import logging
import warnings
from typing import List, Optional

from decouple import config

from util import *

DB_CLIENT_ID = config("DB_CLIENT_ID", None)
DB_API_KEY = config("DB_API_KEY", None)


if not DB_CLIENT_ID or not DB_API_KEY:
    warnings.warn(
        "Deutsche Bahn Parking API disabled! "
        "You need to define DB_CLIENT_ID and DB_API_KEY in environment or in a .env file"
    )

else:

    class BahnParking(ScraperBase):

        POOL = PoolInfo(
            id="bahn",
            name="BizHub // Parking Information // DB Bahnpark 2.3.847",
            public_url="https://data.deutschebahn.com/dataset/api-parkplatz.html",
            source_url="https://apis.deutschebahn.com/db-api-marketplace/apis/parking-information/db-bahnpark/v2/parking-facilities",
            timezone="Europe/Berlin",
            attribution_license="Proprietary Licence DB Bahnpark GmbH",
            attribution_contributor="DB Bahnpark GmbH",
        )

        HEADERS = {
            "DB-Client-Id": f"{DB_CLIENT_ID}",
            "DB-Api-Key": f"{DB_API_KEY}"
        }

        # TODO: This is really not translatable to numbers
        #   that are meaningful in all cases..
        ALLOCATION_TEXT_TO_NUM_FREE_MAPPING = {
            "bis 10": 5,
            ">10": 11,
            ">30": 31,
            ">50": 51,
        }

        FEE_DURATION_MAPPPING = {
            "20min": "20 Minuten",
            "30min": "30 Minuten",
            "1hour": "1 Stunde",
            "1day": "1 Tag",
            "1dayDiscount": "1 Tag rabattiert",
            "1week": "1 Woche",
            "1weekDiscount": "1 Woche rabattiert",
            "1monthVendingMachine": "1 Monat (am Automaten)",
            "1monthLongTerm": "1 Monat Dauerparken (mind. 3 Monate)",
            "1monthReservation": "1 Monat Dauerparken (fester Stellplatz)",
        }


        def __init__(self, caching):
            super().__init__(caching)
            self.log = logging.getLogger(__name__)

        def get_lot_data(self) -> List[LotData]:
            now = self.now()

            facilities = self.get_lot_infos_from_geojson()
            if not facilities:
                return []
                
            lots = []    
            for facility in facilities:
                id = facility.id
                if not facility.has_live_capacity:
                    self.log.debug(f"Parking {id} has no realtime data, skipping." )
                    continue

                if not ', 7' in facility.address:
                    # TODO instead do spatial filtering
                    self.log.debug(f"Parking {id} is not in postal code area 7, skipping." )
                    continue

                prognosis = self.request_json(
                    facility.source_url,
                )
                alloc = prognosis["_embedded"][0]['occupancy']
                
                # --- lot_timestamp ---
                lot_timestamp = alloc.get("timeSegment")
                if lot_timestamp:
                    lot_timestamp = self.to_utc_datetime(lot_timestamp, timezone="utc")   
                
                # --- status ---
                status = LotData.Status.nodata
                if alloc.get("validData"):
                    status = LotData.Status.open

                # --- num free & capacity ---
                capacity = alloc.get("capacity")
                num_free_text = alloc.get("vacancyText")
                num_free = None

                if not num_free_text:
                    if status == LotData.Status.open:
                        status = LotData.Status.error
                else:
                    num_free = self.ALLOCATION_TEXT_TO_NUM_FREE_MAPPING[num_free_text]

                lots.append(
                    LotData(
                        id=id,
                        timestamp=now,
                        lot_timestamp=lot_timestamp,
                        status=status,
                        num_free=num_free,
                        capacity=capacity,
                    )
                )

           return lots

        def get_capacity(self, facility, type = 'PARKING') -> Optional[int]:
            for capacity in facility["capacity"]:
                if capacity['type'] == type and capacity['total'].isnumeric():
                    return int(capacity['total'])
                
            return None
        
        def get_opening_hours(self, facility) -> str:
            facility_opening_hours = facility["access"]["openingHours"]
            if facility_opening_hours["is24h"] == True:
                return "24/7"
            else:
                # TODO Clean up and osm hours validation
                oh_text = facility_opening_hours.get("text", '')
                return oh_text.split('.')[0].split(', Ausfahrt')[0].split('.')[0].replace(': ',' ').replace(' Uhr','').replace(', Sa,So+F geschlossen','').replace(', So+F geschlossen','').replace(' - ','-')

        def get_fee_description(self, facility) -> str:
            fees = filter(lambda fee: fee["price"] != None and fee['group']['groupName']=='standard', facility["tariff"]["prices"])
            fee_strings = [f'{self.FEE_DURATION_MAPPPING[fee["duration"]]}: {fee["price"]:.2f}€' for fee in fees] 
            return ', '.join(fee_strings)
        
        def get_max_stay(self, facility):
            tariffMaxParkingTime = facility["tariff"]["information"]["dynamic"].get("tariffMaxParkingTime")
            return tariffMaxParkingTime

        def get_address(self, facility):
            facility_address =facility["address"]
            zip_and_city = " ".join([facility_address.get("zip"), facility_address.get("city")])
            return ", ".join([facility_address.get("streetAndNumber"), zip_and_city])
        
        def should_ignore(self, facility):
            id = facility["id"]
            if facility["access"]["outOfService"]["isOutOfService"] == True:
                self.log.warning(f'Parking {id} is out of service. Skipping.')
                return True
            if not "capacity" in facility:
                # ignore facilities with no capacity info 
                self.log.warning(f'Parking {id} has no capacity. Skipping.')
                return True
            
        def get_lot_infos(self) -> List[LotInfo]:
            data = self.request_json(
                self.POOL.source_url,
                timeout=60,
            )
            facilities = data["_embedded"]
            
            lots = []
            for facility in facilities:
                if self.should_ignore(facility):
                    # ignore reason is logged by should_ignore
                    continue
                
                id = facility["id"]
                fee_description = self.get_fee_description(facility)

                lots.append(
                    LotInfo(
                        id=name_to_id("db", id),
                        name=facility["name"][1]["name"], # TODO: which context ["DISPLAY"] or NAME?,
                        type=facility["type"]["abbreviation"], # lot type guessing should be sufficient
                        public_url=facility["url"],
                        source_url=f'{self.POOL.source_url}/{id}/prognoses',
                        address=self.get_address(facility),
                        capacity=self.get_capacity(facility, 'PARKING'),
                        capacity_disabled=self.get_capacity(facility, 'HANDICAPPED_PARKING'),
                        capacity_charging= 1 if facility["equipment"]["charging"]["hasChargingStation"] == True else 0,
                        has_live_capacity=facility["hasPrognosis"],
                        latitude=facility["address"]["location"]["latitude"],
                        longitude=facility["address"]["location"]["longitude"],
                        opening_hours = self.get_opening_hours(facility),
                        operator = facility['operator']['name'],
                        max_stay = self.get_max_stay(facility),
                        has_fee = len(fee_description) > 0,
                        fee_description = fee_description,
                        park_ride = True,
                        description = facility["tariff"]["information"]["dynamic"].get("tariffNotes")
                    )
                )

            return lots
