import random
from datetime import datetime, timedelta


class SimulatedMedicalActivityAPI:
    def __init__(self):
        self.medical_event_types = [
            "surgery",
            "opioid prescription",
            "physical therapy",
            "diagnostic imaging",
            "specialist consultation",
            "hospital admission",
            "prescription medication",
            "follow-up visit",
            "emergency room visit",
            "laboratory test",
            "chiropractic treatment",
            "occupational therapy",
        ]

        self.claim_ids = [
            "CLM001",
            "CLM002",
            "CLM003",
            "CLM004",
            "CLM005",
            "CLM006",
            "CLM007",
            "CLM008",
            "CLM009",
            "CLM010",
            "CLM011",
            "CLM012",
            "CLM013",
            "CLM014",
            "CLM015",
            "CLM016",
            "CLM017",
            "CLM018",
            "CLM019",
            "CLM020",
            "CLM021",
            "CLM022",
            "CLM023",
            "CLM024",
            "CLM025",
            "CLM026",
            "CLM027",
            "CLM028",
            "CLM029",
            "CLM030",
            "CLM031",
            "CLM032",
            "CLM033",
            "CLM034",
            "CLM035",
            "CLM036",
            "CLM037",
            "CLM038",
            "CLM039",
            "CLM040",
            "CLM041",
            "CLM042",
            "CLM043",
            "CLM044",
            "CLM045",
            "CLM046",
            "CLM047",
            "CLM048",
            "CLM049",
            "CLM050",
            "CLM051",
            "CLM052",
            "CLM053",
            "CLM054",
            "CLM055",
            "CLM056",
            "CLM057",
            "CLM058",
            "CLM059",
            "CLM060",
        ]

    def request(self):
        claim_id = random.choice(self.claim_ids)
        num_events = random.randint(1, 5)

        # Generate random base date within a reasonable range
        base_date = datetime.now() - timedelta(days=random.randint(30, 365))

        medical_events = []
        for i in range(num_events):
            event_type = random.choice(self.medical_event_types)
            # Generate date offset from base date (events can span multiple days/weeks)
            event_date = base_date + timedelta(days=random.randint(0, 90))
            medical_events.append(
                {"type": event_type, "date": event_date.strftime("%Y-%m-%d")}
            )

        # Sort events by date
        medical_events.sort(key=lambda x: x["date"])

        return {"claim_id": claim_id, "medical_events": medical_events}


if __name__ == "__main__":
    api = SimulatedMedicalActivityAPI()
    result = api.request()
    print(result)
