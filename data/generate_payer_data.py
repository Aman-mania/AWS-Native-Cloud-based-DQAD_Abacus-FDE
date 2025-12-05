"""
Synthetic Payer Claims Data Generator
Generates realistic healthcare payer claims data for DQAD testing
"""

import csv
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict
from faker import Faker

# Initialize Faker
fake = Faker()
Faker.seed(42)
random.seed(42)

# Healthcare-specific code lists
CPT_CODES = [
    "99213", "99214", "99215", "99203", "99204", "99205",  # Office visits
    "99284", "99285", "99283", "99282",  # Emergency room
    "45378", "45380", "45385",  # Colonoscopy
    "93000", "93005", "93010",  # ECG
    "80053", "80061", "85025",  # Lab tests
    "71045", "71046", "71047", "71048",  # Chest X-ray
    "70450", "70460", "70470",  # CT scans
    "77067", "77063",  # Mammography
    "36415", "36416",  # Venipuncture
    "90471", "90472",  # Immunization admin
    "J3301", "J1100", "J2001",  # Drug codes
]

ICD10_CODES = [
    "E11.9",   # Type 2 diabetes without complications
    "I10",     # Essential hypertension
    "E78.5",   # Hyperlipidemia
    "J44.9",   # COPD
    "M54.5",   # Low back pain
    "F41.9",   # Anxiety disorder
    "E66.9",   # Obesity
    "K21.9",   # GERD
    "R05",     # Cough
    "R51",     # Headache
    "N39.0",   # UTI
    "J06.9",   # Upper respiratory infection
    "I25.10",  # Coronary artery disease
    "E03.9",   # Hypothyroidism
    "F32.9",   # Major depressive disorder
    "K76.0",   # Fatty liver
    "M19.90",  # Osteoarthritis
    "H35.30",  # Macular degeneration
    "N18.3",   # Chronic kidney disease
    "C50.919", # Breast cancer
]

DENIAL_REASONS = [
    "Prior authorization required",
    "Service not covered",
    "Duplicate claim",
    "Invalid CPT code",
    "Medical necessity not established",
    "Out of network provider",
    "Patient not eligible",
    "Missing documentation",
    "Timely filing limit exceeded",
    "Incorrect billing code",
]

CLAIM_STATUSES = ["PAID", "DENIED", "PENDING"]
GENDERS = ["M", "F", "U"]  # Male, Female, Unknown


class PayerClaimGenerator:
    """Generate synthetic payer claims data"""
    
    def __init__(self, seed: int = 42, anomaly_rate: float = 0.20):
        """Initialize the generator with a seed for reproducibility
        
        Args:
            seed: Random seed for reproducibility
            anomaly_rate: Percentage of records with anomalies (0.0-1.0, default 0.20 = 20%)
        """
        self.fake = Faker()
        self.anomaly_rate = anomaly_rate
        Faker.seed(seed)
        random.seed(seed)
    
    def generate_npi(self) -> str:
        """Generate a valid-looking 10-digit NPI"""
        return str(random.randint(1000000000, 9999999999))
    
    def generate_claim_amount(self, cpt_code: str) -> float:
        """Generate realistic claim amounts based on CPT code"""
        # Different procedure types have different cost ranges
        if cpt_code.startswith("99"):  # Office/ER visits
            return round(random.uniform(100, 500), 2)
        elif cpt_code.startswith("45"):  # Procedures
            return round(random.uniform(1000, 5000), 2)
        elif cpt_code.startswith("93"):  # Cardiac tests
            return round(random.uniform(200, 1500), 2)
        elif cpt_code.startswith("80") or cpt_code.startswith("85"):  # Labs
            return round(random.uniform(50, 300), 2)
        elif cpt_code.startswith("70") or cpt_code.startswith("71"):  # Imaging
            return round(random.uniform(500, 3000), 2)
        elif cpt_code.startswith("77"):  # Mammography
            return round(random.uniform(200, 800), 2)
        elif cpt_code.startswith("36"):  # Blood draw
            return round(random.uniform(25, 100), 2)
        elif cpt_code.startswith("90"):  # Vaccines
            return round(random.uniform(30, 150), 2)
        elif cpt_code.startswith("J"):  # Drugs
            return round(random.uniform(100, 5000), 2)
        else:
            return round(random.uniform(100, 1000), 2)
    
    def generate_service_date(self, days_back: int = 90) -> str:
        """Generate a random service date within the last N days"""
        start_date = datetime.now() - timedelta(days=days_back)
        random_days = random.randint(0, days_back)
        service_date = start_date + timedelta(days=random_days)
        return service_date.strftime("%Y-%m-%d")
    
    def generate_submission_date(self, service_date: str) -> str:
        """Generate submission date (typically 1-30 days after service)"""
        service_dt = datetime.strptime(service_date, "%Y-%m-%d")
        days_delay = random.randint(1, 30)
        submission_dt = service_dt + timedelta(days=days_delay)
        return submission_dt.strftime("%Y-%m-%d")
    
    def introduce_anomalies(self, claim: Dict, anomaly_rate: float = None) -> Dict:
        """Introduce data quality anomalies for testing
        
        Args:
            claim: Claim dictionary to potentially modify
            anomaly_rate: Override default anomaly rate (uses self.anomaly_rate if None)
        """
        rate = anomaly_rate if anomaly_rate is not None else self.anomaly_rate
        if random.random() < rate:
            anomaly_type = random.choice([
                "invalid_npi",
                "negative_amount",
                "future_date",
                "invalid_cpt",
                "missing_diagnosis",
                "duplicate_claim",
                "excessive_amount",
            ])
            
            if anomaly_type == "invalid_npi":
                claim["provider_npi"] = "0000000000"
            elif anomaly_type == "negative_amount":
                claim["claim_amount"] = -abs(claim["claim_amount"])
            elif anomaly_type == "future_date":
                future_date = datetime.now() + timedelta(days=random.randint(1, 30))
                claim["service_date"] = future_date.strftime("%Y-%m-%d")
            elif anomaly_type == "invalid_cpt":
                claim["cpt_code"] = "INVALID"
            elif anomaly_type == "missing_diagnosis":
                claim["icd10_code"] = ""
            elif anomaly_type == "excessive_amount":
                claim["claim_amount"] = claim["claim_amount"] * random.uniform(10, 100)
        
        return claim
    
    def generate_claim(self, member_id: str = None, include_anomalies: bool = True) -> Dict:
        """Generate a single payer claim"""
        if member_id is None:
            member_id = f"MBR{random.randint(100000, 999999)}"
        
        provider_id = f"PRV{random.randint(10000, 99999)}"
        cpt_code = random.choice(CPT_CODES)
        claim_status = random.choices(
            CLAIM_STATUSES, 
            weights=[0.75, 0.15, 0.10]  # 75% paid, 15% denied, 10% pending
        )[0]
        
        service_date = self.generate_service_date()
        
        claim = {
            "claim_id": str(uuid.uuid4()),
            "member_id": member_id,
            "provider_id": provider_id,
            "provider_npi": self.generate_npi(),
            "cpt_code": cpt_code,
            "icd10_code": random.choice(ICD10_CODES),
            "claim_amount": self.generate_claim_amount(cpt_code),
            "service_date": service_date,
            "submission_date": self.generate_submission_date(service_date),
            "claim_status": claim_status,
            "denial_reason": random.choice(DENIAL_REASONS) if claim_status == "DENIED" else "",
            "patient_dob": self.fake.date_of_birth(minimum_age=18, maximum_age=90).strftime("%Y-%m-%d"),
            "patient_zip": self.fake.zipcode(),
            "patient_gender": random.choice(GENDERS),
        }
        
        # Introduce anomalies for testing
        if include_anomalies:
            claim = self.introduce_anomalies(claim)
        
        return claim
    
    def generate_batch(self, num_claims: int, include_anomalies: bool = True) -> List[Dict]:
        """Generate a batch of claims"""
        claims = []
        
        # Generate claims for unique members (some members have multiple claims)
        num_members = int(num_claims * 0.6)  # 60% unique members
        member_ids = [f"MBR{random.randint(100000, 999999)}" for _ in range(num_members)]
        
        for i in range(num_claims):
            # Some members have multiple claims
            if i < num_members:
                member_id = member_ids[i]
            else:
                member_id = random.choice(member_ids)
            
            claim = self.generate_claim(member_id, include_anomalies)
            claims.append(claim)
        
        return claims
    
    def save_to_csv(self, claims: List[Dict], output_path: str):
        """Save claims to CSV file"""
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        fieldnames = [
            "claim_id", "member_id", "provider_id", "provider_npi",
            "cpt_code", "icd10_code", "claim_amount", "service_date",
            "submission_date", "claim_status", "denial_reason",
            "patient_dob", "patient_zip", "patient_gender"
        ]
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(claims)
        
        print(f"✓ Generated {len(claims)} claims and saved to {output_path}")
    
    def generate_daily_batch(self, output_dir: str, num_claims: int = 1000, 
                           include_anomalies: bool = True):
        """Generate a daily batch file with timestamp"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"payer_claims_{timestamp}.csv"
        output_path = Path(output_dir) / filename
        
        claims = self.generate_batch(num_claims, include_anomalies)
        self.save_to_csv(claims, str(output_path))
        
        return str(output_path)


def main():
    """Main function to generate sample data"""
    # Initialize with 35% anomaly rate for alarm/self-healing testing
    # Use 0.35-0.40 to reliably trigger CloudWatch alarms and orchestrator
    generator = PayerClaimGenerator(anomaly_rate=0.35)  
    # Change to 0.05-0.10 for normal testing, 0.35-0.40 for alarm testing
    
    # Generate multiple batches for testing
    output_dir = Path(__file__).parent.parent / "raw_data"
    
    print("Generating synthetic payer claims data...")
    print("=" * 60)
    
    # Generate 3 daily batches
    for i in range(3):
        num_claims = random.randint(800, 1200)
        file_path = generator.generate_daily_batch(
            output_dir=str(output_dir),
            num_claims=num_claims,
            include_anomalies=True
        )
        print(f"Batch {i+1}: {num_claims} claims")
    
    print("=" * 60)
    print("✓ Data generation complete!")
    print(f"Files saved to: {output_dir}")
    print("\nNote: ~5% of claims contain intentional anomalies for testing")


if __name__ == "__main__":
    main()
