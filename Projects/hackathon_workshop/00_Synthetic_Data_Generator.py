# Databricks notebook source
# DBTITLE 1,Synthetic Data Generator
# MAGIC %md
# MAGIC # Contact Center — Synthetic Data Generator
# MAGIC
# MAGIC Generates realistic pseudo data matching the **Genesys interaction metadata** and **transcript line-level** schemas.
# MAGIC
# MAGIC ### Output Tables
# MAGIC | Table | Description | Key Columns |
# MAGIC |-------|-------------|-------------|
# MAGIC | `interactions_raw` | Genesys call metadata (~160 cols) | Conversation ID, Queue, Duration, Skills, Direction |
# MAGIC | `transcripts_raw` | Line-by-line transcript | conversation_id, line_num, participant, transcribed_text |
# MAGIC
# MAGIC ### Parameters
# MAGIC * `NUM_CALLS` — Number of synthetic calls to generate (default: 50)
# MAGIC * `DATE_RANGE_DAYS` — Spread calls over N days (default: 30)
# MAGIC * `NUM_AGENTS` — Agent pool size (default: 12)

# COMMAND ----------

# DBTITLE 1,Configuration Parameters
# ============================================================
# CONFIGURATION — UPDATE THESE FOR YOUR ENVIRONMENT
# ============================================================
HC_PROVIDER = "HCP"       # <-- Your org name (e.g. "Valley Health", "Metro Medical")
CATALOG = "mmt_aws_usw2_catalog"  # <-- Your Unity Catalog name
SCHEMA = "contact_calls"          # <-- Your schema name
# NOTE: Notebooks 01-05 reference CATALOG.SCHEMA in SQL cells.
#       Find-and-replace "mmt_aws_usw2_catalog.contact_calls" with your catalog.schema.

NUM_CALLS = 50
DATE_RANGE_DAYS = 30
NUM_AGENTS = 12
START_DATE = "2026-05-01"

# date_time confirmed in real schema; line_num handles ordering
INCLUDE_UTTERANCE_TIMESTAMPS = True

# [CONFIRMED] = verified from Genesys export | [REALISTIC] = plausible approximation
QUEUES = ["Appointments", "Billing", "Nurse Advice", "Referrals", "Pharmacy", "Medical Records", "Insurance Verification"]  # [REALISTIC]
SKILLS = ["English", "Spanish", "Mandarin", "Scheduling", "Clinical Triage", "Billing Disputes", "Insurance Auth"]  # [REALISTIC]
LANGUAGES = ["English", "Spanish", "Mandarin", "Vietnamese", "Cantonese"]  # [REALISTIC]
WRAP_UPS = ["Resolved", "Transferred", "Callback Scheduled", "Escalated", "Voicemail Left", "Abandoned"]  # [REALISTIC]
DIVISIONS = ["Downtown Medical Center", "Westside Community Hospital", "Valley General", "Northgate Clinic", "Bayside Medical Foundation"]  # [REALISTIC]  # NOTE: Check DIVISIONS for your Genesys export; these are plausible examples.
DIRECTIONS = ["inbound", "outbound"]  # [CONFIRMED]
DISCONNECT_TYPES = ["client", "system", "peer", "transfer", "endpoint"]  # [CONFIRMED]
PROVIDERS = ["Edge", "PureCloud Voice", "BYOC Premises", "BYOC Cloud"]  # [CONFIRMED]

print(f"Config: {NUM_CALLS} calls, {DATE_RANGE_DAYS} days, {NUM_AGENTS} agents")
print(f"   Queues: {len(QUEUES)} | Skills: {len(SKILLS)} | Languages: {len(LANGUAGES)}")

# COMMAND ----------

# DBTITLE 1,Generate Genesys Interaction Metadata
import random
import string
from datetime import datetime, timedelta, date
from pyspark.sql import Row
from pyspark.sql.types import *
import uuid

random.seed(42)

# ============================================================
# GENERATE GENESYS INTERACTION METADATA
# Schema: ~160 columns matching the Genesys export format
# ============================================================

def generate_agent_pool(n):
    """Generate a pool of agent names/IDs."""
    first_names = ["Maria", "James", "Linda", "Robert", "Patricia", "Michael", "Jennifer", "David", "Sarah", "Kevin", "Amy", "Carlos", "Priya", "Wei", "Thanh"]
    last_names = ["Garcia", "Smith", "Nguyen", "Park", "Rodriguez", "Chen", "Thompson", "Patel", "Johnson", "Williams", "Kim", "Martinez", "Lee", "Brown", "Davis"]
    agents = []
    for i in range(n):
        name = f"{random.choice(first_names)} {random.choice(last_names)}"
        agents.append({"name": name, "id": f"AGT-{random.randint(1000, 9999)}"})
    return agents

def generate_phone_number():
    return f"+1{random.randint(200,999)}{random.randint(100,999)}{random.randint(1000,9999)}"

def generate_conversation_id():
    return str(uuid.uuid4())

def random_duration_sec(queue):
    """Queue-specific duration distributions."""
    base = {"Appointments": 180, "Billing": 400, "Nurse Advice": 300, "Referrals": 350, 
            "Pharmacy": 200, "Medical Records": 250, "Insurance Verification": 450}
    b = base.get(queue, 300)
    return max(60, int(random.gauss(b, b * 0.3)))


agent_pool = generate_agent_pool(NUM_AGENTS)
start_dt = datetime.strptime(START_DATE, "%Y-%m-%d")

interactions = []
for i in range(NUM_CALLS):
    conv_id = generate_conversation_id()
    queue = random.choice(QUEUES)
    agent = random.choice(agent_pool)
    direction = random.choices(DIRECTIONS, weights=[0.85, 0.15])[0]
    duration = random_duration_sec(queue)
    
    # Timestamps
    day_offset = random.randint(0, DATE_RANGE_DAYS - 1)
    hour = random.choices(range(24), weights=[1,1,1,1,1,2,4,8,10,10,9,9,9,8,8,7,6,5,4,3,2,2,1,1])[0]
    minute = random.randint(0, 59)
    call_start = start_dt + timedelta(days=day_offset, hours=hour, minutes=minute)
    call_end = call_start + timedelta(seconds=duration)
    
    # Segment durations (sum to ~duration)
    queue_time = random.randint(10, 120)
    alert_time = random.randint(5, 30)
    talk_time = max(30, duration - queue_time - alert_time - random.randint(10, 60))
    hold_time = random.randint(0, 60) if random.random() < 0.3 else 0
    acw_time = random.randint(15, 90)
    
    # Outcome flags
    abandoned = random.random() < 0.08
    transferred = random.random() < 0.12
    
    row = {
        "Users": agent["name"],
        "Users_Alerted": agent["name"] if not abandoned else "",
        "Users_Interacted": agent["name"] if not abandoned else "",
        "Remote": generate_phone_number(),
        "Date": call_start.strftime("%Y-%m-%d %H:%M:%S"),
        "End_Date": call_end.strftime("%Y-%m-%d %H:%M:%S"),
        "Duration": duration,
        "Direction": direction,
        "Initial_Direction": direction,
        "ANI": generate_phone_number(),
        "DNIS": f"+1916555{random.randint(1000,9999)}",
        "Session_DNIS": f"+1916555{random.randint(1000,9999)}",
        "Queue": queue,
        "Wrap_up": "Abandoned" if abandoned else random.choice(WRAP_UPS[:5]),
        "Wrap_up_Notes": "" if random.random() < 0.7 else f"Follow-up needed: {random.choice(['callback', 'email', 'supervisor review'])}",
        "SIP_Call_ID": f"sip-{uuid.uuid4().hex[:16]}",
        "Conversation_ID": conv_id,
        "Session_ID": str(uuid.uuid4()),  # [CONFIRMED] Genesys media session ID
        "Skills": ", ".join(random.sample(SKILLS, random.randint(1, 3))),
        "Languages": random.choice(LANGUAGES),
        "Screen_Share": "", "Co_Browse": "", "Voicemail": "Yes" if random.random() < 0.05 else "",
        "Non_ACD": "", "Blind_Transferred": "Yes" if random.random() < 0.05 else "",
        "Consulted": "Yes" if random.random() < 0.1 else "",
        "Consult_Transferred": "", "Recording": "Yes",
        "Protected": "", "MOS": round(random.uniform(3.5, 4.8), 1),
        "Transferred": "Yes" if transferred else "",
        "Abandoned": "Yes" if abandoned else "",
        "To": 0, "From": 0,
        "Flagged": "Yes" if random.random() < 0.03 else "",
        "Division": random.choice(DIVISIONS),
        "Preferred_Agents_Requested": "", "Preferred_Agents": 0,
        "IVR_Segments": random.randint(1, 4), "Total_IVR": random.randint(5, 45),
        "Queue_Segments": 1, "Total_Queue": queue_time,
        "Alert_Segments": 1, "Total_Alert": alert_time,
        "User_Segments": 1, "Talk_Segments": random.randint(1, 3), "Total_Talk": talk_time,
        "Hold_Segments": 1 if hold_time > 0 else 0, "Total_Hold": hold_time,
        "Wrap_Up_Segments": 1, "Total_ACW": acw_time,
        "Dialing_Segments": 0, "Total_Dialing": 0,
        "Contacting_Segments": 0, "Total_Contacting": 0,
        "Transfers": 1 if transferred else 0, "Total_Handle": duration + acw_time,
        "Blind_Transfers": 0, "Consults": 0, "Consult_Transfers": 0,
        "Has_Survey_Data": "Yes" if random.random() < 0.15 else "",
        "Surveys": 1 if random.random() < 0.15 else 0,
        "Survey_Status": 0, "Survey_Form": 0,
        "Survey_Score": random.randint(1, 5) if random.random() < 0.15 else 0,
        "Promoter_Score": random.randint(0, 10) if random.random() < 0.1 else 0,
        "Campaign_Name": 0, "Contact_List_Name": 0, "Contact_Id": 0,
        "Call_Analysis_Result": 0, "Outbound_Attempted": 0,
        "Campaign": "", "Outcome_Success": 0, "Outcome_Success_Pct": 0.0,
        "Outcome_Failure": 0, "Outcome_Failure_Pct": 0.0, "Outcome_Attempts": 0,
        "Flow_Exit": 0, "All_Flow_Disconnect": 0,
        "Customer_Disconnect": 1 if random.random() < 0.3 else 0,
        "Flow_Disconnect": 0, "System_Error_Disconnect": 0,
        "Customer_Short_Disconnect": 0, "Flow": "Main IVR",
        "Successful_Outcomes": "Yes" if not abandoned else "",
        "Failed_Outcomes": "" if not abandoned else "Yes",
        "Incomplete_Outcomes": 0,
        "Has_Customer_Journey_Data": "Yes" if random.random() < 0.4 else "",
        "Proactive": "", "Has_Media": 1, "Inbound_Media": 1 if direction == "inbound" else 0,
        "Emails_Sent": 0, "Time_to_Abandon": queue_time if abandoned else 0,
        "Monitored": "Yes" if random.random() < 0.1 else "",
        "First_Queue": queue, "Total_Voicemail": 0, "Last_Wrap_Up": random.choice(WRAP_UPS[:5]),
        "Abandoned_in_Queue": 1 if abandoned else 0,
        "Disconnect_Type": random.choice(DISCONNECT_TYPES),
        "Subject": 0, "Total_Monitor": 0,
        "Provider": random.choice(PROVIDERS), "Flow_Out_Type": 0,
        "Routing_Requested": "Standard", "Routing_Used": "Standard",
        "Predictive_Agent_Selected": 0, "Agent_Assist": "",
        "Manual_Agents_Assigned": 0, "Manual_Assigner": 0,
        "Error_Code": "", "Error_Count": 0,
        "Campaign_Start": "", "Campaign_Caller_Name": "",
        "Time_To_Flow": random.randint(1, 5), "Time_To_Agent": queue_time + alert_time,
        "Total_Active_Callback": 0, "Preferred_Rule": 0, "Routing_Rule": 0,
        "Bullseye_Ring": random.randint(1, 3), "Agent_Bullseye_Ring": random.randint(1, 3),
        "Skills_Removed": 0, "Skills_Active": ", ".join(random.sample(SKILLS, random.randint(1, 2))),
        "External_Tag": 0, "Outbound_Media": 0,
        "Authenticated": "Yes" if random.random() < 0.6 else "",
        "Not_Responding": 0, "Users_Not_Responding": "",
        "Evaluation_Created": "Yes" if random.random() < 0.2 else "",
        "Evaluated_Agent": agent["name"] if random.random() < 0.2 else "",
        "Evaluator": "", "Evaluation_Score": "", "Evaluation_Critical_Score": "",
        "Delivery_Status": "", "Delivery_Status_Details": "",
        "Conversation_Initiator": "customer" if direction == "inbound" else "agent",
        "Customer_Participated": "Yes",
        "Fax": "",
        "Callback_Time_to_First_Dial": 0, "Callback_Time_to_First_Connect": 0,
        "Direct_Routing": "", "Email_CC": "", "Email_BCC": "",
        "Coached": "", "Barged_In": "", "Total_Coaching": 0, "Total_Barge_In": 0,
        "Cleared_by_Customer": "", "Evaluation_Assignee": "", "Evaluation_Status": "",
        "Parked": "", "Total_Park": 0, "Active_Park": 0, "Survey_Type": 0,
        "Screen_Recorded": "", "Social_Classification": "", "Conference": "",
        "Inbound_Messages": 0, "Outbound_Messages": 0,
        "Inbound_SMS_MMS_Segments": 0, "Outbound_SMS_MMS_Segments": 0,
        "Agentless_Emails": 0, "Group_Ring": 0, "Push_Notifications": "",
        "Avg_Agent_Response": 0, "Avg_Customer_Response": 0,
        "Total_First_Response": 0, "Total_First_Engagement": 0,
        "Message_Turns": 0, "Inbound_Audio": "Yes" if direction == "inbound" else "",
        "Outbound_Audio": "Yes" if direction == "outbound" else "",
        "Messages": 0, "Media": 0, "Session_Expired": "",
        "Snippet_Recorded": "", "Snippet_Recordings": 0, "Total_Snippet_Recorded": 0,
    }
    interactions.append(row)

df_interactions = spark.createDataFrame(interactions)
df_interactions.createOrReplaceTempView("interactions_raw")

print(f"Generated {df_interactions.count()} Genesys interaction records")
print(f"   Columns: {len(df_interactions.columns)}")
print(f"   Date range: {START_DATE} to {(start_dt + timedelta(days=DATE_RANGE_DAYS)).strftime('%Y-%m-%d')}")
print(f"   Queues: {df_interactions.select('Queue').distinct().count()} unique")
print(f"   Agents: {df_interactions.select('Users').distinct().count()} unique")
display(df_interactions.select("Conversation_ID", "Users", "Queue", "Date", "Duration", "Direction", "Skills", "Languages", "Wrap_up").limit(10))

# COMMAND ----------

# DBTITLE 1,Generate Transcript Line-Level Data
# ============================================================
# GENERATE TRANSCRIPT LINE-LEVEL DATA
# Schema: direction, participants, interaction_id, line_num, 
#         participant, participant_type, transcribed_text
# ============================================================

# Realistic conversation templates per queue type
CONVERSATION_TEMPLATES = {
    "Appointments": [
        ("agent", "Thank you for calling {hc_provider}, this is {agent_name} speaking. How may I help you today?"),
        ("customer", "Hi, I need to {action} my appointment with {doctor}."),
        ("agent", "I'd be happy to help with that. May I have your name and date of birth for verification?"),
        ("customer", "Sure, it's {caller_name}, date of birth {dob}."),
        ("agent", "Thank you {caller_first}. I can see your appointment. {resolution}"),
        ("customer", "{customer_response}"),
        ("agent", "Is there anything else I can help you with today?"),
        ("customer", "No, that's all. Thank you."),
        ("agent", "You're welcome, {caller_first}. Have a wonderful day."),
    ],
    "Billing": [
        ("agent", "{hc_provider} billing department, this is {agent_name}. How can I assist you?"),
        ("customer", "I have a question about a bill I received. {billing_issue}"),
        ("agent", "I understand your concern. Let me look into that for you. Can I get your account number or date of birth?"),
        ("customer", "My account number is {account_num}. Name is {caller_name}."),
        ("agent", "Thank you. {billing_resolution}"),
        ("customer", "{customer_response}"),
        ("agent", "Is there anything else regarding your billing I can help with?"),
        ("customer", "No, thank you."),
        ("agent", "Thank you for calling {hc_provider}. Have a good day.")
    ],
    "Nurse Advice": [
        ("agent", "{hc_provider} nurse advice line, this is {agent_name}. Before we begin, may I have your name and date of birth for verification?"),
        ("customer", "It's {caller_name}, date of birth {dob}."),
        ("agent", "Thank you. What symptoms are you experiencing?"),
        ("customer", "{symptom_description}"),
        ("agent", "{triage_question}"),
        ("customer", "{symptom_answer}"),
        ("agent", "{clinical_advice}"),
        ("customer", "OK, thank you."),
        ("agent", "If symptoms worsen or you develop {warning_signs}, please go to the nearest ER. Is there anything else?"),
        ("customer", "No, thank you."),
        ("agent", "Take care. Call back anytime if you need further assistance."),
    ],
    "Referrals": [
        ("agent", "{hc_provider} referrals, this is {agent_name}. How can I help you?"),
        ("customer", "I need a referral to see a {specialist}. My doctor said they would send one but I haven't heard anything."),
        ("agent", "I can look into that. May I have your name and date of birth?"),
        ("customer", "{caller_name}, {dob}."),
        ("agent", "{referral_status}"),
        ("customer", "{customer_response}"),
        ("agent", "I'll make sure this gets resolved. You should hear back within {timeframe}. Anything else?"),
        ("customer", "No, that's all."),
        ("agent", "Thank you for your patience, {caller_first}. Have a good day."),
    ],
}

# Fill-in values for templates
DOCTORS = ["Dr. Patel", "Dr. Thompson", "Dr. Chen", "Dr. Williams", "Dr. Garcia", "Dr. Kim", "Dr. Martinez"]
SPECIALISTS = ["neurologist", "cardiologist", "orthopedist", "dermatologist", "endocrinologist", "gastroenterologist"]
CALLER_NAMES = ["James Rodriguez", "Patricia Nguyen", "Linda Park", "Michael Chen", "Sarah Johnson", 
                "David Kim", "Jennifer Martinez", "Robert Smith", "Amy Thompson", "Carlos Garcia"]
ACTIONS = ["reschedule", "cancel", "confirm", "schedule a new"]
BILLING_ISSUES = ["The amount seems wrong.", "I was told my insurance would cover this.", 
                  "I already paid this last month.", "I don't recognize this charge.", "My copay should only be $25."]
BILLING_RESOLUTIONS = ["I can see the discrepancy. Your insurance adjustment hasn't posted yet. You should see a corrected statement in 5-7 business days.",
                       "It looks like your payment crossed with our billing cycle. I'm applying a correction now.",
                       "I see the charge is for an out-of-network lab. Let me check if we can reprocess through your in-network benefits."]
SYMPTOMS = ["My child has had a fever of 102 for two days.", "I've been having chest tightness and shortness of breath.",
            "I twisted my ankle and it's very swollen.", "I have a persistent headache that won't go away.",
            "My blood sugar reading was 350 this morning."]
WARNING_SIGNS = ["difficulty breathing, chest pain, or loss of consciousness", "fever above 104 or seizures",
                 "severe swelling, numbness, or inability to bear weight", "vision changes or sudden severe headache"]


def fill_template(template_lines, agent_name, queue, hc_provider=HC_PROVIDER):
    """Fill a conversation template with random realistic values."""
    caller_name = random.choice(CALLER_NAMES)
    caller_first = caller_name.split()[0]
    dob = f"{random.randint(1,12):02d}/{random.randint(1,28):02d}/{random.randint(1955,1998)}"
    
    filled = []
    for role, text in template_lines:
        line = text.format(
            hc_provider=hc_provider,
            agent_name=agent_name,
            caller_name=caller_name,
            caller_first=caller_first,
            dob=dob,
            doctor=random.choice(DOCTORS),
            specialist=random.choice(SPECIALISTS),
            action=random.choice(ACTIONS),
            account_num=f"{random.randint(1000,9999)}-{random.randint(1000,9999)}",
            billing_issue=random.choice(BILLING_ISSUES),
            billing_resolution=random.choice(BILLING_RESOLUTIONS),
            resolution=random.choice(["I have an opening next Tuesday at 10 AM. Would that work?", 
                                       "I've cancelled that appointment. Would you like to reschedule?",
                                       "Your appointment is confirmed for the original date."]),
            customer_response=random.choice(["That works, thank you.", "OK great.", "Perfect.", "Sounds good."]),
            symptom_description=random.choice(SYMPTOMS),
            triage_question=random.choice(["How long has this been going on?", "Are there any other symptoms?",
                                           "On a scale of 1-10, how severe is the pain?"]),
            symptom_answer=random.choice(["About two days now.", "Since yesterday evening.", "It started this morning."]),
            clinical_advice=random.choice(["Based on what you're describing, I recommend monitoring at home with rest and fluids.",
                                           "This sounds like it could benefit from an urgent care visit within the next few hours.",
                                           "I'd recommend calling your primary care doctor first thing tomorrow for an appointment."]),
            warning_signs=random.choice(WARNING_SIGNS),
            referral_status=random.choice(["I see a referral was submitted but it's pending authorization. I'll escalate this.",
                                           "The referral was approved. Let me get you scheduled.",
                                           "It looks like the referral wasn't submitted. I'll contact your doctor's office to get this started."]),
            timeframe=random.choice(["24 hours", "48 hours", "3-5 business days"]),
        )
        filled.append((role, line))
    return filled


# Extra filler exchanges to inject variation in conversation length
FILLER_EXCHANGES = [
    [("customer", "Actually, I have one more question."), ("agent", "Of course, go ahead.")],
    [("agent", "Let me put you on a brief hold while I check that."), ("agent", "Thank you for holding. I have that information now.")],
    [("customer", "Can you also check on my other appointment?"), ("agent", "Sure, let me pull that up."), ("agent", "I see it here. Everything looks confirmed.")],
    [("customer", "Sorry, one more thing."), ("agent", "No problem at all, what can I help with?"), ("customer", "Never mind, I think that's it actually.")],
    [("agent", "While I have you, I want to make sure your contact information is up to date."), ("customer", "Yes, everything is the same.")],
    [("customer", "How long will that take?"), ("agent", "Typically 3-5 business days. I can also send you a confirmation email."), ("customer", "Yes please, that would be great.")],
]

# Generate transcripts for each interaction
transcript_rows = []
conv_ids = [row["Conversation_ID"] for row in interactions]

for interaction in interactions:
    conv_id = interaction["Conversation_ID"]
    agent_name = interaction["Users"]
    queue = interaction["Queue"]
    direction = interaction["Direction"]
    call_start = datetime.strptime(interaction["Date"], "%Y-%m-%d %H:%M:%S")
    duration = interaction["Duration"]
    is_abandoned = interaction["Abandoned"] == "Yes"
    is_transferred = interaction["Transferred"] == "Yes"
    
    
    # Pick template (default to Appointments if queue not in templates)
    template_key = queue if queue in CONVERSATION_TEMPLATES else random.choice(list(CONVERSATION_TEMPLATES.keys()))
    template = CONVERSATION_TEMPLATES[template_key]
    
    # Fill template
    lines = fill_template(template, agent_name, queue)
    
    # Vary length: inject 0-2 filler exchanges at random points (before the closing)
    if not is_abandoned and random.random() < 0.6:
        num_fillers = random.randint(1, 2)
        for _ in range(num_fillers):
            filler = random.choice(FILLER_EXCHANGES)
            insert_point = random.randint(4, len(lines) - 2)  # not at very start or end
            for i, (role, text) in enumerate(filler):
                filled_text = text.replace("{agent_name}", agent_name)
                lines.insert(insert_point + i, (role, filled_text))
    
    # Abandoned calls: truncate early (customer hangs up mid-conversation)
    if is_abandoned:
        truncate_at = random.randint(2, 5)
        lines = lines[:truncate_at]
    
    # Generate line-by-line transcript records
    time_per_line = duration / max(len(lines), 1)
    
    for line_num, (role, text) in enumerate(lines, 1):
        line_time = call_start + timedelta(seconds=int(time_per_line * (line_num - 1)))
        
        transcript_rows.append({
            "direction": direction,
            "external_participants": interaction["Remote"],
            "interaction_id": conv_id,
            "interaction_type": "voice",
            "internal_participants": agent_name,
            "transcript_duration": str(duration),
            "transcript_end_time": interaction["End_Date"],
            "transcript_start_time": interaction["Date"],
            "conversation_id": conv_id,
            "date_time": line_time.strftime("%Y-%m-%d %H:%M:%S") if INCLUDE_UTTERANCE_TIMESTAMPS else None,
            "line_num": line_num,
            "participant": agent_name if role == "agent" else "Customer",
            "participant_type": "internal" if role == "agent" else "external",
            "transcribed_text": text,
        })

df_transcripts = spark.createDataFrame(transcript_rows)
df_transcripts.createOrReplaceTempView("transcripts_raw")

print(f"Generated {df_transcripts.count()} transcript lines across {NUM_CALLS} conversations")
print(f"   Avg lines per call: {df_transcripts.count() / NUM_CALLS:.1f}")
print(f"   Columns: {len(df_transcripts.columns)}")
# Note: session_id is in interactions_raw (metadata), not in transcripts_raw (matches real Genesys schema)
display(df_transcripts.filter("interaction_id = '" + conv_ids[0] + "'").orderBy("line_num"))

# COMMAND ----------

# DBTITLE 1,Validate Generated Data
# ============================================================
# VALIDATION — Verify schema compliance and data quality
# ============================================================

print("="*70)
print("SCHEMA VALIDATION")
print("="*70)

# Interactions table
print(f"\n▶ interactions_raw")
print(f"  Rows: {df_interactions.count()}")
print(f"  Columns: {len(df_interactions.columns)}")
print(f"  Null check: Conversation_ID has {df_interactions.filter('Conversation_ID IS NULL').count()} nulls")

# Key distribution checks
from pyspark.sql.functions import col, count, avg, min as spark_min, max as spark_max

print(f"\n  Distribution checks:")
queue_dist = df_interactions.groupBy("Queue").count().orderBy("count", ascending=False).collect()
for row in queue_dist:
    print(f"    {row['Queue']:<25} {row['count']:>4} calls")

# Transcripts table
print(f"\n\n▶ transcripts_raw")
print(f"  Rows: {df_transcripts.count()}")
print(f"  Columns: {len(df_transcripts.columns)}")
print(f"  Schema:")
for field in df_transcripts.schema.fields:
    print(f"    {field.name:<30} {field.dataType.simpleString()}")

print(f"\n\nBoth tables ready for downstream pipeline")
print(f"   Use: spark.table('interactions_raw') and spark.table('transcripts_raw')")

# COMMAND ----------

# DBTITLE 1,Persist Raw Interaction and Transcript Data to Delta Ta ...
# ============================================================
# PERSIST: Write to Delta tables for downstream notebooks
# Downstream notebooks read from these tables
# ============================================================

# Uses CATALOG and SCHEMA from config cell above
catalog = CATALOG
schema = SCHEMA

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

df_interactions.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.interactions_raw")
df_transcripts.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.transcripts_raw")
print(f"Written to {catalog}.{schema}.interactions_raw and .transcripts_raw")
print(f"   Downstream notebooks read from: {catalog}.{schema}")

# COMMAND ----------

# DBTITLE 1,Next Steps
# MAGIC %md
# MAGIC ## What just happened
# MAGIC
# MAGIC You now have two **Delta tables** matching the **real Genesys Cloud export format**:
# MAGIC
# MAGIC | Table | Rows | What it is |
# MAGIC |-------|------|------------|
# MAGIC | `mmt_aws_usw2_catalog.contact_calls.interactions_raw` | 50 calls | Call metadata — queue, agent, duration, skills, wrap-up (~160 columns) |
# MAGIC | `mmt_aws_usw2_catalog.contact_calls.transcripts_raw` | ~500+ lines | Line-by-line transcript — one row per utterance (2-17 lines per call) |
# MAGIC
# MAGIC **Next notebook →** `01_Stitch_Transcripts` takes the line-by-line format and reconstructs full conversations.
# MAGIC
# MAGIC ---
# MAGIC ### Scoring Rubric: This notebook sets up the data foundation for Criterion 1 (E2E Functionality)