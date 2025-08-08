import requests
import pandas as pd
from datetime import datetime
import json
import os
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font
import sys

class HubSpotExporter:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://api.hubapi.com"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        self.debug = True
    
    def test_connection(self):
        """Test the API connection and validate the API key"""
        print("\n🔍 Testing HubSpot API connection...")
        print(f"   API Key: {self.api_key[:20]}..." if len(self.api_key) > 20 else f"   API Key: {self.api_key}")
        
        # Test with a simple API call
        url = f"{self.base_url}/crm/v3/objects/contacts?limit=1"
        
        try:
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 200:
                print("✅ API connection successful!")
                data = response.json()
                total_contacts = data.get("total", 0)
                print(f"   Total contacts in HubSpot: {total_contacts}")
                return True
            elif response.status_code == 401:
                print("\n❌ Authentication failed! (401 Error)")
                print("\n   TROUBLESHOOTING STEPS:")
                print("   1. Verify you're using a Private App Access Token (not an API key)")
                print("   2. Private App tokens should start with 'pat-na1-'")
                print("   3. Check if the token has expired")
                print("   4. Ensure the Private App has necessary permissions:")
                print("      - crm.objects.contacts.read")
                print("      - crm.lists.read")
                print("\n   TO GET A NEW TOKEN:")
                print("   1. Go to HubSpot > Settings > Integrations > Private Apps")
                print("   2. Create a new app or use existing one")
                print("   3. Set scopes: Contacts (Read), Lists (Read)")
                print("   4. Copy the Access Token")
                return False
            else:
                print(f"❌ API error: {response.status_code}")
                print(f"   Response: {response.text}")
                return False
        except Exception as e:
            print(f"❌ Connection error: {str(e)}")
            return False
    
    def get_lists(self):
        """Fetch all available lists"""
        print("\n📋 Fetching available lists...")
        
        # Try both v1 and v3 endpoints
        endpoints = [
            f"{self.base_url}/contacts/v1/lists",
            f"{self.base_url}/crm/v3/lists"
        ]
        
        for url in endpoints:
            try:
                response = requests.get(url, headers=self.headers)
                
                if response.status_code == 200:
                    if "/v1/" in url:
                        lists = response.json().get("lists", [])
                    else:
                        lists = response.json().get("results", [])
                    
                    if lists:
                        print(f"\n✅ Found {len(lists)} lists:")
                        print("-" * 70)
                        for lst in lists:
                            if "/v1/" in url:
                                print(f"Name: {lst.get('name', 'Unknown')}")
                                print(f"ID: {lst.get('listId', 'Unknown')}")
                                print(f"Contact Count: {lst.get('metaData', {}).get('size', 0)}")
                            else:
                                print(f"Name: {lst.get('name', 'Unknown')}")
                                print(f"ID: {lst.get('hs_list_id', lst.get('listId', 'Unknown'))}")
                                print(f"Contact Count: {lst.get('hs_list_size', 0)}")
                            print("-" * 70)
                        return lists
                elif response.status_code == 401:
                    print(f"❌ Authentication failed for {url}")
                    continue
            except Exception as e:
                print(f"   Error with {url}: {str(e)}")
                continue
        
        print("❌ Could not fetch lists from any endpoint")
        return []
    
    def get_contacts_from_list(self, list_id, limit=100):
        """Fetch contacts from a specific list"""
        contacts = []
        offset = 0
        page = 1
        
        print(f"\n📥 Fetching contacts from list {list_id}...")
        
        while True:
            url = f"{self.base_url}/contacts/v1/lists/{list_id}/contacts/all"
            params = {
                "count": limit,
                "vidOffset": offset,
                "property": ["email", "firstname", "lastname", "phone", "company", "lifecyclestage", "hs_lead_status"]
            }
            
            if self.debug:
                print(f"   Fetching page {page} (offset: {offset})...")
            
            try:
                response = requests.get(url, headers=self.headers, params=params)
                
                if response.status_code == 401:
                    print(f"❌ Authentication failed! Please check your API token.")
                    return []
                elif response.status_code == 404:
                    print(f"❌ List ID {list_id} not found!")
                    print("   Run exporter.get_lists() to see available lists.")
                    return []
                elif response.status_code != 200:
                    print(f"❌ Error {response.status_code}: {response.text}")
                    return []
                
                data = response.json()
                page_contacts = data.get("contacts", [])
                contacts.extend(page_contacts)
                
                if self.debug:
                    print(f"   Retrieved {len(page_contacts)} contacts")
                
                if data.get("has-more", False):
                    offset = data.get("vid-offset", 0)
                    page += 1
                else:
                    break
                    
            except Exception as e:
                print(f"❌ Exception occurred: {str(e)}")
                break
        
        print(f"\n✅ Total contacts retrieved: {len(contacts)}")
        
        # Convert to v3 format
        v3_contacts = []
        for contact in contacts:
            v3_contact = {
                "id": str(contact.get("vid", "")),
                "properties": {}
            }
            
            # Flatten properties
            props = contact.get("properties", {})
            for prop, value in props.items():
                if isinstance(value, dict):
                    v3_contact["properties"][prop] = value.get("value", "")
                else:
                    v3_contact["properties"][prop] = value
            
            v3_contacts.append(v3_contact)
        
        return v3_contacts
    
    def load_existing_data(self, filepath):
        """Load existing Excel file and return dataframes for each sheet"""
        existing_data = {}
        
        if os.path.exists(filepath):
            print(f"📂 Loading existing file: {filepath}")
            try:
                excel_file = pd.ExcelFile(filepath)
                for sheet_name in excel_file.sheet_names:
                    df = pd.read_excel(filepath, sheet_name=sheet_name)
                    existing_data[sheet_name] = df
                    print(f"   ✓ Loaded sheet '{sheet_name}' with {len(df)} rows")
            except Exception as e:
                print(f"⚠️  Error loading existing file: {e}")
                return {}
        
        return existing_data
    
    def merge_contact_data(self, new_contacts, existing_contacts):
        """Merge new contact data with existing, preserving custom columns"""
        if existing_contacts is None or existing_contacts.empty:
            return pd.DataFrame(new_contacts)
        
        # Convert to DataFrames
        new_df = pd.DataFrame(new_contacts)
        existing_df = existing_contacts.copy()
        
        # Check if existing data has Contact ID column
        if 'Contact ID' not in existing_df.columns:
            print("   ⚠️  No 'Contact ID' column in existing data, returning new data only")
            return new_df
        
        # Identify custom columns
        hubspot_columns = set(new_df.columns) if not new_df.empty else set()
        custom_columns = [col for col in existing_df.columns if col not in hubspot_columns]
        
        if custom_columns:
            print(f"   📝 Preserving custom columns: {custom_columns}")
        
        # If new_df is empty, keep existing structure
        if new_df.empty:
            print("   ⚠️  No new data to merge")
            return existing_df
        
        # Create mapping of Contact ID to existing custom data
        custom_data = existing_df[['Contact ID'] + custom_columns].set_index('Contact ID').to_dict('index')
        
        # Add Last Updated column
        new_df['Last Updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Add custom columns to new data
        for col in custom_columns:
            new_df[col] = new_df['Contact ID'].map(lambda x: custom_data.get(x, {}).get(col, ''))
        
        # Identify changes
        existing_ids = set(existing_df['Contact ID'])
        new_ids = set(new_df['Contact ID'])
        added_ids = new_ids - existing_ids
        
        if added_ids:
            print(f"   ✨ Found {len(added_ids)} new contacts")
        
        return new_df
    
    def update_excel(self, filename="hubspot_export.xlsx", list_id=None, list_name=None, 
                    merge_strategy='update', track_changes=True):
        """Update existing Excel file or create new one with HubSpot data"""
        
        # First, test the connection
        if not self.test_connection():
            print("\n⚠️  Cannot proceed without valid authentication.")
            print("   Please update your API token and try again.")
            return False
        
        # Get the directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        full_filepath = os.path.join(script_dir, filename)
        
        # Load existing data
        existing_data = self.load_existing_data(full_filepath)
        file_exists = bool(existing_data)
        
        # Get contacts
        if list_id:
            contacts = self.get_contacts_from_list(list_id)
            export_type = f"List {list_name or list_id}"
        else:
            print("\n📥 Fetching all contacts...")
            contacts = self.get_contacts_from_list("all")  # Use "all" for all contacts
            export_type = "All Contacts"
        
        # Prepare data
        contact_data = []
        email_data = []
        meeting_data = []
        
        # Process contacts
        for i, contact in enumerate(contacts):
            if (i + 1) % 10 == 0:
                print(f"   Processing contact {i+1}/{len(contacts)}...")
            
            contact_id = contact.get("id", "")
            properties = contact.get("properties", {})
            
            contact_info = {
                "Contact ID": contact_id,
                "Email": properties.get("email", ""),
                "First Name": properties.get("firstname", ""),
                "Last Name": properties.get("lastname", ""),
                "Phone": properties.get("phone", ""),
                "Company": properties.get("company", ""),
                "Lifecycle Stage": properties.get("lifecyclestage", ""),
                "Lead Status": properties.get("hs_lead_status", ""),
                "Last Updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            contact_data.append(contact_info)
        
        # Handle merging
        if file_exists and merge_strategy == 'update' and 'Contacts' in existing_data:
            print("\n🔄 Merging with existing data...")
            contact_df = self.merge_contact_data(contact_data, existing_data['Contacts'])
        else:
            contact_df = pd.DataFrame(contact_data) if contact_data else pd.DataFrame(columns=["Contact ID", "Email", "First Name", "Last Name", "Phone", "Company", "Lifecycle Stage", "Lead Status", "Last Updated"])
        
        email_df = pd.DataFrame(email_data) if email_data else pd.DataFrame(columns=["Contact ID", "Contact Email", "Contact Name", "Email Subject", "Email Direction", "Email Status", "Email Date", "Email Preview"])
        meeting_df = pd.DataFrame(meeting_data) if meeting_data else pd.DataFrame(columns=["Contact ID", "Contact Email", "Contact Name", "Meeting Title", "Meeting Start", "Meeting End", "Meeting Outcome", "Meeting Notes"])
        
        # Create summary
        summary_df = contact_df.copy() if not contact_df.empty else pd.DataFrame()
        if not summary_df.empty:
            summary_df["Total Emails"] = 0
            summary_df["Total Meetings"] = 0
            summary_df["Last Email Date"] = ""
            summary_df["Last Meeting Date"] = ""
        
        # Write to Excel
        print(f"\n💾 Saving to: {full_filepath}")
        
        try:
            with pd.ExcelWriter(full_filepath, engine='openpyxl', mode='w') as writer:
                # Export Info
                metadata = pd.DataFrame([{
                    "Export Date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "Export Type": export_type,
                    "Total Contacts": len(contact_df),
                    "Total Emails": len(email_df),
                    "Total Meetings": len(meeting_df),
                    "Status": "Updated" if file_exists else "Created",
                    "Authentication": "Success"
                }])
                metadata.to_excel(writer, sheet_name='Export Info', index=False)
                
                # Data sheets
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
                contact_df.to_excel(writer, sheet_name='Contacts', index=False)
                email_df.to_excel(writer, sheet_name='Emails', index=False)
                meeting_df.to_excel(writer, sheet_name='Meetings', index=False)
            
            print(f"\n✅ Export complete!")
            print(f"📊 Summary:")
            print(f"   - {len(contact_df)} contacts in file")
            
            # Open folder
            if os.name == 'nt':
                os.startfile(script_dir)
            
            return True
            
        except Exception as e:
            print(f"\n❌ Error writing Excel file: {str(e)}")
            return False
    
    def format_timestamp(self, timestamp):
        """Convert HubSpot timestamp to readable date"""
        if not timestamp:
            return ""
        try:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            return timestamp


def main():
    """Main function with menu"""
    print("=" * 70)
    print("HUBSPOT TO EXCEL EXPORTER")
    print("=" * 70)
    
    # You can set this as environment variable for security
    API_KEY = os.environ.get('HUBSPOT_API_KEY', '')
    
    if not API_KEY:
        print("\n⚠️  No API key found in environment variable HUBSPOT_API_KEY")
        API_KEY = input("Please enter your HubSpot Private App token: ").strip()
    
    if not API_KEY:
        print("❌ No API key provided. Exiting.")
        return
    
    exporter = HubSpotExporter(API_KEY)
    
    # Test connection first
    if not exporter.test_connection():
        return
    
    while True:
        print("\n" + "=" * 70)
        print("MENU:")
        print("1. List all available HubSpot lists")
        print("2. Export contacts from a specific list")
        print("3. Export ALL contacts")
        print("4. Exit")
        print("=" * 70)
        
        choice = input("\nEnter your choice (1-4): ").strip()
        
        if choice == '1':
            exporter.get_lists()
            input("\nPress Enter to continue...")
            
        elif choice == '2':
            list_id = input("\nEnter the List ID: ").strip()
            list_name = input("Enter a friendly name for this list (optional): ").strip()
            filename = input("Enter filename (default: hubspot_export.xlsx): ").strip() or "hubspot_export.xlsx"
            
            exporter.update_excel(
                filename=filename,
                list_id=list_id,
                list_name=list_name
            )
            
        elif choice == '3':
            filename = input("\nEnter filename (default: all_contacts.xlsx): ").strip() or "all_contacts.xlsx"
            exporter.update_excel(filename=filename)
            
        elif choice == '4':
            print("\nGoodbye!")
            break
        
        else:
            print("\n❌ Invalid choice. Please try again.")


if __name__ == "__main__":
    # Option 1: Run interactive menu
    main()
    
    # Option 2: Direct usage (uncomment below)
    # API_KEY = "your-token-here"
    # exporter = HubSpotExporter(API_KEY)
    # exporter.update_excel(
    #     filename="my_contacts.xlsx",
    #     list_id="2",
    #     list_name="My List"
    # )