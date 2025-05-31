import os
import requests
import json
import time
import pandas as pd
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from queue import Queue
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
SLACK_API_TOKEN = os.getenv('SLACK_API_TOKEN')
CHANNEL_ID = os.getenv('CHANNEL_ID')
CLAY_WEBHOOK_URL = os.getenv('CLAY_WEBHOOK_URL')

# File to store last processed timestamp
LAST_PROCESSED_FILE = 'last_processed_timestamp.txt'

# Threading configuration
MAX_WORKERS = 10
RATE_LIMIT_DELAY = 0.1

class ThreadSafeCounter:
    def __init__(self):
        self._value = 0
        self._lock = threading.Lock()
    
    def increment(self):
        with self._lock:
            self._value += 1
            return self._value
    
    @property
    def value(self):
        return self._value

# Technical titles list
TECHNICAL_TITLES = [
    "Solutions Architect",
    "Senior Vice President Of Engineering",
    "Senior Applied Scientist",
    "Software Engineer",
    "IT Consultant",
    "Senior Principal System Engineer",
    "Senior Software Engineer",
    "Lead Software Developer",
    "Business Zone Technician",
    "data analyst/roi",
    "Sr Solutions Architect",
    "Quality Assurance Engineer",
    "Senior Bioinformatics Developer",
    "System Administrator",
    "Senior Product Designer - Facebook for Business & Commerce",
    "Senior Technology Leader for Texas Instruments",
    "Sr. Magento Developer/Lead",
    "Senior Application Analyst",
    "Sr. Engineering Manager",
    "Pipeline Designer V",
    "Senior Full Stack Developer",
    "Principal Software Engineer - Advertising",
    "Senior SRE Leader | Driving Automation, Innovation, & Scalability | Empowering Mid-Sized Companies with Reliability, Availability, & Performance Across Infrastructure, DevOps, & Cloud",
    "Principal Product Manager",
    "Senior Computer Scientist",
    "Technical Lead IV",
    "Website Marketing Manager & WordPress Developer",
    "Technical Lead/ Software Engineer lll",
    "Composition Software Engineering (GMC) - Manager",
    "Application Solution Specialist",
    "Project Engineer Intermediate - AV Operation Support Tier 2 / Tier 3",
    "Sr Developer",
    "Data Analyst",
    "Senior Vice President -  Digital/Data, Product and Technology Platforms",
    "Business Architecture Manager",
    "Junior Data Analyst",
    "UX Writer & Content Designer",
    "Information Technology Administrator",
    "Lead Software Engineer",
    "Senior 5G systems integration engineer",
    "Software(Application) Architect",
    "Technical Specialist",
    "Software Consultant",
    "Senior Salesforce Developer",
    "Director of Software Engineering",
    "Software Engineering Advisor",
    "System Engineer",
    "Principal Software Architect",
    "Python Developer",
    "Senior Security Engineer",
    "Software Engineer, Machine Learning",
    "Specialist Data Center Manager",
    "Software Developer",
    "Senior Network Architect",
    "Developer Advocate",
    "Site Support Engineer",
    "Sr. Anlst, IT&D Eng & Arch",
    "Sr. SEO Manager",
    "Desktop Support Analyst",
    "Senior Director - Technology Solutions",
    "Talos Threat Hunt Team Lead (Cyber Shaman, Lvl 31)",
    "Tester",
    "Principal Engineer",
    "Sr. Quality Project Analyst",
    "Big Data Consultant",
    "Director, Cloud DevOps/SRE & DBA",
    "MTS 1 Software Engineer",
    "Software Engineering Manager",
    "Chief Technology Officer",
    "Test Engineer",
    "Senior Clinical Business Analyst",
    "Senior Manager, Content Design team",
    "Software engineer",
    "Senior Software Engineer in Test",
    "Senior Developer Relations Engineer",
    "Data Engineer",
    "Engineering Manager, Web",
    "Lead Developer",
    "Senior Manager of Software Development",
    "Frontend Developer",
    "Lead Machine Learning Engineer",
    "Manager, UX Design",
    "user experience design manager product team",
    "Software and Systems Engineer",
    "Lead Support Programmer",
    "System Engineer, Project Manager",
    "SVP of Software",
    "Senior Technical Product Manager",
    "Senior Network Engineer",
    "Senior Product Manager",
    "Business System Analyst",
    "Programming Analyst",
    "Regulatory Integration Engineer - Safety Regulations & Certification",
    "Senior Sales Engineer",
    "Sr Technology Director",
    "Engineering Manager - Apple Media Products",
    "Technical Consultant",
    "Sr Software Engineer",
    "Senior Business System Analyst",
    "Manager, Marketing Applications Architect",
    "Senior Software Engineer III",
    "Senior Facilities Validation Engineer",
    "Staff Database Engineer",
    "Hack4Impact JDT Lead",
    "Product Manager, VR Search",
    "Principle IT Engineer Applications",
    "Application Developer",
    "Principal Software Engineer",
    "Co-Founder / CTO",
    "Client Platform Engineer",
    "Software QA Engineer",
    "Senior Development Tester",
    "PRINCIPAL SOFTWARE DEVELOPER",
    "Senior Infrastructure Engineer",
    "Senior Design Engineer",
    "Product @ RelationalAI",
    "Staff Software Engineer",
    "VP - Strategic Project Portfolio Management and New Product Development",
    "Senior Machine Learning Scientist",
    "Software Programmer",
    "Director, Technical Coaching",
    "AVP Enterprise IT Operations",
    "Senior Technology & Data Partner, TCS Interactive",
    "Senior Staff Software Engineer @ Google | PhD in Computer Science",
    "Senior UX/UI Software Architect",
    "Platform Support Manager - Global Integrations",
    "Senior Product Support Engineer",
    "Senior Engineer, Cloud Financial Operations",
    "Associate Developer",
    "Server & Application Administrator",
    "R & D Senior Member of Technical Staff",
    "Senior Developer",
    "Customer Technical Support",
    "Freelance Web Developer/Photographer/Graphic Designer",
    "Cloud computing | Digital Projects Transformation | Application Development",
    "Information Technology Business System Analyst",
    "Director of Engineering",
    "Technical Program Manager 3",
    "Sr. Computer Scientist",
    "Solution Architect",
    "Desktop Support",
    "Head of Life Sciences - Business Technology Architecture",
    "Sr Director of Engineering & Site Lead",
    "Analog IC Design Engineer",
    "Engineering Specialist",
    "Application Architect",
    "Product Manager",
    "Senior Siebel Consultant",
    "QA Analyst",
    "Vice President Of Software Development",
    "Performance Test Lead at Roche Diagnostics - Actively looking for new position",
    "Senior System Analyst",
    "Technology Manager",
    "Sr. Director of Engineering",
    "Accessibility support technician",
    "site reliability engineer",
    "Systems / Network Admin",
    "Senior Developer",
    "Staff Engineer",
    "Principal Software Engineer",
    "Senior Data Engineer",
    "Senior Programmer Analyst",
    "Manufacturing Project Engineer",
    "Principal Manager",
    "Computer Software Consultant and Professional",
    "Senior 3d Generalist",
    "Technical Manager at Capgemini",
    "Director Of Electrical Engineering",
    "Senior Engineering Manager",
    "Software developer",
    "Software Engineering Technical Lead",
    "Software Developer",
    "IT Senior Manager, Solutions Architecture",
    "Developer",
    "It Manager Infrastructure And Support",
    "Senior Java Developer",
    "IBM Executive Certified Project Manager",
    "VP of Consumer Engineering",
    "Information Technology Specialist",
    "Principal Product Manager - Tech",
    "Systems Analyst",
    "Senior Developer",
    "Information Technology Network Engineer",
    "Senior UI Architect / Dev Lead at General Motors",
    "Technology Development Specialist",
    "iPhone Application Developer (contracted)",
    "Sr. Lead Software Engineer",
    "Vice President and Cio",
    "Senior Manager - Software Engineering",
    "Lead Systems Engineer",
    "Technical Advisor",
    "Principal Member of Technical Staff - Software Development",
    "Software Engineering Fellow",
    "Owner-IT Consultant, M.P.I., Inc.",
    "Software Engineer Iii - Full Stack",
    "Chief Engineer, Integration and Production Program Directorate",
    "Technical Support Engineer",
    "administrative program coordinator, financial operations",
    "Cloud Solutions Architect",
    "director of product management",
    "Senior Business Intelligence Analyst, Global Supply Management and Logistics Data & Analytics",
    "head of global data and advanced analytics",
    "Calibration Specialist/Purchasing Manager",
    "Sr. Marketing Operations Mngr | Salesforce Certified 2x",
    "Senior Director, Client Solutions",
    "Automation Engineer at GrayMatter",
    "Digital and Content Strategist",
    "MIS Instructor",
    "Technical Lead",
    "Principal Engineer"
]

def get_last_processed_timestamp():
    """Get the last processed timestamp from file"""
    try:
        if os.path.exists(LAST_PROCESSED_FILE):
            with open(LAST_PROCESSED_FILE, 'r') as f:
                return float(f.read().strip())
    except:
        pass
    return None

def save_last_processed_timestamp(timestamp):
    """Save the last processed timestamp to file"""
    try:
        with open(LAST_PROCESSED_FILE, 'w') as f:
            f.write(str(timestamp))
    except Exception as e:
        print(f"Error saving timestamp: {e}")

def get_channel_messages(channel_id, oldest_timestamp=None):
    """Fetch messages from Slack channel since last processed timestamp"""
    url = 'https://slack.com/api/conversations.history'
    headers = {
        'Authorization': f'Bearer {SLACK_API_TOKEN}',
        'Content-Type': 'application/json;charset=utf-8'
    }
    params = {
        'channel': channel_id,
        'limit': 100
    }
    
    # Only get messages newer than last processed timestamp
    if oldest_timestamp:
        params['oldest'] = str(oldest_timestamp)

    all_messages = []
    has_more = True
    
    while has_more:
        try:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code != 200:
                print(f"Error fetching messages: {response.status_code}")
                return None

            data = response.json()
            if not data['ok']:
                print(f"Slack API Error: {data['error']}")
                return None

            messages = data.get('messages', [])
            all_messages.extend(messages)
            has_more = data.get('has_more', False)
            
            if has_more:
                params['cursor'] = data['response_metadata']['next_cursor']
                time.sleep(1)
                
        except Exception as e:
            print(f"Exception while fetching messages: {e}")
            return None

    return all_messages

def extract_visitor_info(text):
    """Extract visitor information from Slack message text"""
    if not text or not isinstance(text, str):
        return {}
    
    info = {}
    
    patterns = {
        'Name': r'\*Name\*:\s*(.*?)\s*(?:\*|$)',
        'Title': r'\*Title\*:\s*(.*?)\s*(?:\*|$)',
        'Company': r'\*Company\*:\s*(.*?)\s*(?:\*|$)',
        'Email': r'\*Email\*:\s*(.*?)\s*(?:\*|$)',
        'LinkedIn': r'\*LinkedIn\*:\s*(.*?)\s*(?:\*|$)',
        'Location': r'\*Location\*:\s*(.*?)\s*(?:\*|$)',
        'First_Identified_Visiting': r'First identified visiting \*<(.*?)>\*',
        'Visit_Time': r'on \*(.*?)\*',
        'About': r'\*About <(.*?)\|',
        'Website': r'\*Website\*:\s*<(.*?)>',
        'Est_Employees': r'\*Est\. Employees\*:\s*(.*?)\s*(?:\*|$)',
        'Industry': r'\*Industry\*:\s*(.*?)\s*(?:\*|$)',
        'Est_Revenue': r'\*Est\. Revenue\*:\s*(.*?)\s*(?:\*|$)'
    }
    
    for field, pattern in patterns.items():
        match = re.search(pattern, text)
        info[field] = match.group(1).strip() if match else None
    
    return info

def clean_linkedin_url(url):
    """Clean and validate LinkedIn URLs"""
    if not url or not isinstance(url, str):
        return None
    
    url = url.replace('www.http://', '').replace('www.https://', '')
    url = url.replace('http://http://', 'http://').replace('https://https://', 'https://')
    
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    if 'linkedin.com/in/' not in url.lower():
        return None
        
    return url

def send_to_clay_with_retry(visitor_record, max_retries=3):
    """Send individual visitor record to Clay webhook with retry logic"""
    for attempt in range(max_retries):
        try:
            time.sleep(RATE_LIMIT_DELAY)
            
            response = requests.post(
                CLAY_WEBHOOK_URL,
                json=visitor_record,
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            
            if response.status_code == 200:
                return {
                    'success': True,
                    'name': visitor_record.get('Name', 'Unknown'),
                    'message': 'Success'
                }
            elif response.status_code == 429:
                wait_time = (2 ** attempt) * RATE_LIMIT_DELAY
                print(f"Rate limited, waiting {wait_time}s before retry {attempt + 1}")
                time.sleep(wait_time)
                continue
            else:
                return {
                    'success': False,
                    'name': visitor_record.get('Name', 'Unknown'),
                    'message': f"HTTP {response.status_code}: {response.text[:100]}"
                }
                
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                print(f"Timeout for {visitor_record.get('Name', 'Unknown')}, retrying...")
                time.sleep(1)
                continue
            return {
                'success': False,
                'name': visitor_record.get('Name', 'Unknown'),
                'message': 'Timeout after retries'
            }
        except Exception as e:
            return {
                'success': False,
                'name': visitor_record.get('Name', 'Unknown'),
                'message': f"Error: {str(e)}"
            }
    
    return {
        'success': False,
        'name': visitor_record.get('Name', 'Unknown'),
        'message': 'Failed after all retries'
    }

def send_batch_to_clay_threaded(visitors_list):
    """Send all visitors to Clay using multithreading"""
    if not visitors_list:
        return 0, []
        
    total_visitors = len(visitors_list)
    success_counter = ThreadSafeCounter()
    failed_records = []
    
    print(f"Starting to send {total_visitors} new visitors to Clay...")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_visitor = {
            executor.submit(send_to_clay_with_retry, visitor): visitor 
            for visitor in visitors_list
        }
        
        for future in as_completed(future_to_visitor):
            result = future.result()
            
            if result['success']:
                count = success_counter.increment()
                if count % 5 == 0:
                    print(f"Progress: {count}/{total_visitors} sent successfully")
            else:
                failed_records.append(result)
                print(f"Failed to send {result['name']}: {result['message']}")
    
    success_count = success_counter.value
    failed_count = len(failed_records)
    
    print(f"Completed! Successfully sent: {success_count}, Failed: {failed_count}")
    return success_count, failed_records

def process_and_send_visitors():
    """Main function to process new Slack messages and send to Clay"""
    print(f"Starting incremental sync at {datetime.now()}")
    
    # Get last processed timestamp
    last_timestamp = get_last_processed_timestamp()
    if last_timestamp:
        print(f"Processing messages since: {datetime.fromtimestamp(last_timestamp)}")
    else:
        print("First run - processing all messages")
    
    # Fetch messages
    messages = get_channel_messages(CHANNEL_ID, last_timestamp)
    
    if not messages:
        print("No new messages retrieved from Slack")
        return
    
    print(f"Retrieved {len(messages)} new messages")
    
    # Find the latest timestamp for next run
    latest_timestamp = max(float(msg['ts']) for msg in messages) if messages else last_timestamp
    
    # Process messages
    df = pd.DataFrame(messages)
    bot_messages = df[df.get('subtype') == 'bot_message']
    
    if bot_messages.empty:
        print("No new bot messages found")
        # Still update timestamp to avoid reprocessing
        if latest_timestamp:
            save_last_processed_timestamp(latest_timestamp)
        return
    
    print(f"Found {len(bot_messages)} new bot messages")
    
    # Extract visitor information
    visitor_data = []
    for _, message in bot_messages.iterrows():
        text = message.get('text', '')
        extracted_info = extract_visitor_info(text)
        
        if extracted_info and extracted_info.get('Name'):
            if 'ts' in message:
                extracted_info['slack_timestamp'] = message['ts']
                extracted_info['processed_date'] = datetime.now().isoformat()
            visitor_data.append(extracted_info)
    
    if not visitor_data:
        print("No visitor data extracted from new messages")
        if latest_timestamp:
            save_last_processed_timestamp(latest_timestamp)
        return
    
    print(f"Extracted data for {len(visitor_data)} new visitors")
    
    # Filter for technical professionals
    if TECHNICAL_TITLES:
        technical_visitors = []
        for visitor in visitor_data:
            title = visitor.get('Title', '')
            if title and any(tech_title.lower() in title.lower() for tech_title in TECHNICAL_TITLES):
                technical_visitors.append(visitor)
        
        print(f"Found {len(technical_visitors)} new technical professionals")
        visitor_data = technical_visitors
    
    # Clean LinkedIn URLs and filter
    for visitor in visitor_data:
        if visitor.get('LinkedIn'):
            visitor['LinkedIn'] = clean_linkedin_url(visitor['LinkedIn'])
    
    linkedin_visitors = [v for v in visitor_data if v.get('LinkedIn')]
    print(f"Found {len(linkedin_visitors)} new visitors with valid LinkedIn profiles")
    
    # Prepare clean data
    clean_visitors = []
    for visitor in linkedin_visitors:
        cleaned_visitor = {k: v for k, v in visitor.items() if v is not None and v != ''}
        if cleaned_visitor:
            clean_visitors.append(cleaned_visitor)
    
    if not clean_visitors:
        print("No clean visitor data to send")
        if latest_timestamp:
            save_last_processed_timestamp(latest_timestamp)
        return
    
    # Send to Clay
    start_time = time.time()
    success_count, failed_records = send_batch_to_clay_threaded(clean_visitors)
    end_time = time.time()
    
    print(f"\n=== SUMMARY ===")
    print(f"Processing time: {end_time - start_time:.2f} seconds")
    print(f"Successfully sent: {success_count} new records")
    print(f"Failed: {len(failed_records)} records")
    
    # Save data for reference
    if clean_visitors:
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        df_final = pd.DataFrame(clean_visitors)
        df_final.to_csv(f'visitors_sent_to_clay_{timestamp_str}.csv', index=False)
        print(f"Data saved to 'visitors_sent_to_clay_{timestamp_str}.csv'")
    
    # Update timestamp for next run
    if latest_timestamp:
        save_last_processed_timestamp(latest_timestamp)
        print(f"Updated last processed timestamp to: {datetime.fromtimestamp(latest_timestamp)}")

def main():
    """Entry point"""
    try:
        if not all([SLACK_API_TOKEN, CHANNEL_ID, CLAY_WEBHOOK_URL]):
            print("Error: Missing required environment variables")
            return
        
        process_and_send_visitors()
    except Exception as e:
        print(f"Error in main process: {e}")

if __name__ == "__main__":
    main()