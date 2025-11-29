#   contracts = (
#                     db.query(Company_To_Client_Contract)
#                     .filter(Company_To_Client_Contract.client_id == 349)
#                     .all()
#                 )

#                 for contract in contracts:
#                     # pull the COD‐rate row (should be exactly one per contract)
#                     cod = (
#                         db.query(Company_To_Client_COD_Rates)
#                         .filter_by(contract_id=contract.id)
#                         .first()
#                     )

#                     # pull all the zone‐rate rows
#                     zone_rows = (
#                         db.query(Company_To_Client_Rates)
#                         .filter_by(contract_id=contract.id)
#                         .all()
#                     )

#                     # initialize all zone columns to None
#                     data = {
#                         f"base_rate_zone_{z}": None for z in ["a", "b", "c", "d", "e"]
#                     }
#                     data.update(
#                         {
#                             f"additional_rate_zone_{z}": None
#                             for z in ["a", "b", "c", "d", "e"]
#                         }
#                     )
#                     data.update(
#                         {
#                             f"rto_base_rate_zone_{z}": None
#                             for z in ["a", "b", "c", "d", "e"]
#                         }
#                     )
#                     data.update(
#                         {
#                             f"rto_additional_rate_zone_{z}": None
#                             for z in ["a", "b", "c", "d", "e"]
#                         }
#                     )

#                     # pivot the zone rows into the columns
#                     for zr in zone_rows:
#                         z = zr.zone.lower()
#                         data[f"base_rate_zone_{z}"] = zr.base_rate
#                         data[f"additional_rate_zone_{z}"] = zr.additional_rate
#                         data[f"rto_base_rate_zone_{z}"] = zr.rto_base_rate
#                         data[f"rto_additional_rate_zone_{z}"] = zr.rto_additional_rate

#                     # build the new denormalized record
#                     new_rec = New_Company_To_Client_Rate(
#                         uuid=(uuid4()),  # preserve cod_rate.uuid if present
#                         created_at=cod.created_at if cod else datetime.utcnow(),
#                         updated_at=cod.updated_at if cod else datetime.utcnow(),
#                         is_deleted=cod.is_deleted if cod else False,
#                         rate_type=contract.rate_type,  # or another enum/string you use
#                         percentage_rate=cod.percentage_rate if cod else None,
#                         absolute_rate=cod.absolute_rate if cod else None,
#                         isActive=contract.isActive,  # or contract.is_active
#                         company_id=1,
#                         client_id=contract.client_id,
#                         company_contract_id=contract.company_contract_id,
#                         aggregator_courier_id=contract.aggregator_courier_id,
#                         # zone‐based freight/rto rates
#                         **data,
#                     )

#                     db.add(new_rec)

#                 db.commit()
#                 db.close()
#                 print(f"Done: migrated rates for {len(contracts)} contracts.")

#                 return GenericResponseModel(
#                     status_code=http.HTTPStatus.OK,
#                     message="Order cloned successfully",
#                     status=True,
#                 )


# client_id = 93

# orders = (
#     db.query(Order)
#     .filter(
#         Order.client_id == client_id,
#         Order.status != "new",
#         Order.status != "cancelled",
#         Order.tracking_info != None,
#     )
#     .all()
# )

# # print(orders)

# # return

# for order in orders[1:]:

#     try:

#         tracking_info = order.tracking_info

#         if tracking_info is None or len(tracking_info) == 0:
#             continue

#         body = {
#             "awb": order.awb_number,
#             "current_status": order.sub_status,
#             "order_id": order.order_id,
#             "current_timestamp": (
#                 order.tracking_info[0]["datetime"]
#                 if order.tracking_info
#                 else order.booking_date.strftime("%d-%m-%Y %H:%M:%S")
#             ),
#             "shipment_status": order.sub_status,
#             "scans": [
#                 {
#                     "datetime": activity["datetime"],
#                     "status": activity["status"],
#                     "location": activity["location"],
#                 }
#                 for activity in order.tracking_info
#             ],
#         }

#         response = requests.post(
#             url="https://lg3ksh1zw3.execute-api.ap-south-1.amazonaws.com/prod/webhook/bluedart",
#             verify=True,
#             timeout=10,
#             json=body,
#         )

#         print(response.json())
#         print(order.order_id)

#     except:
#         continue

# from modules.shipping_notifications.shipping_notifications_service import (
#     ShippingNotificaitions,
# )

# # phones = [
# #     phone[2:] if phone.startswith("91") else phone for phone in phones
# # ]

# phones = []

# orders = (
#     db.query(Order).filter(
#         Order.booking_date >= date(2025, 2, 10),  # Proper date format
#         Order.client_id == 68,
#         ~Order.consignee_phone.in_(
#             phones
#         ),  # Exclude phones in the list
#     )
#     # .limit(3)
#     .all()
# )

# for order in orders:
#     ShippingNotificaitions.send_notification(order, "order_shipped")

# return


# # Step 1: Load and normalize 'surface.xlsx'
# df_surface = pd.read_excel("surface.xlsx")
# df_surface.columns = [col.strip().lower() for col in df_surface.columns]
# df_surface.rename(
#     columns={"destination pincode": "pincode"}, inplace=True
# )
# df_surface["pincode"] = df_surface["pincode"].astype(int)
# df_surface["prepaid"] = (
#     df_surface["prepaid"]
#     .astype(str)
#     .str.strip()
#     .str.lower()
#     .map({"y": True, "n": False})
# )
# df_surface["cod"] = (
#     df_surface["cod"]
#     .astype(str)
#     .str.strip()
#     .str.lower()
#     .map({"y": True, "n": False})
# )

# # Step 2: Load and normalize 'air.xlsx'
# df_air = pd.read_excel("air.xlsx")
# df_air.columns = [col.strip().lower() for col in df_air.columns]
# df_air.rename(columns={"destination pincode": "pincode"}, inplace=True)
# df_air["pincode"] = df_air["pincode"].astype(int)
# df_air["prepaid"] = (
#     df_air["prepaid"]
#     .astype(str)
#     .str.strip()
#     .str.lower()
#     .map({"y": True, "n": False})
#     .fillna(False)
# )
# df_air["cod"] = (
#     df_air["cod"]
#     .astype(str)
#     .str.strip()
#     .str.lower()
#     .map({"y": True, "n": False})
#     .fillna(False)
# )

# # Step 3: Merge both DataFrames on pincode
# df_merged = pd.merge(
#     df_surface,
#     df_air,
#     on="pincode",
#     suffixes=("_surface", "_air"),
#     how="outer",
# )

# # Step 4: Get pincode -> id mapping from DB
# pincode_to_id = dict(
#     db.query(
#         Pincode_Serviceability.pincode, Pincode_Serviceability.id
#     ).all()
# )

# # Step 5: Prepare and perform bulk update with batching
# update_data = []
# batch_size = 1000
# total_rows = len(df_merged)
# processed_count = 0

# for _, row in df_merged.iterrows():
#     pincode = row["pincode"]
#     if pincode in pincode_to_id:
#         update_data.append(
#             {
#                 "id": pincode_to_id[pincode],
#                 "dtdc_surface_lm_cod": row.get("cod_surface"),
#                 "dtdc_surface_lm_prepaid": row.get("prepaid_surface"),
#                 "dtdc_air_lm_cod": row.get("cod_air"),
#                 "dtdc_air_lm_prepaid": row.get("prepaid_air"),
#             }
#         )
#     else:
#         print(f"⚠️ Skipping pincode not in DB: {pincode}")

#     if len(update_data) >= batch_size:
#         db.bulk_update_mappings(Pincode_Serviceability, update_data)
#         db.commit()
#         processed_count += len(update_data)
#         print(
#             f"🛠️ Updated {processed_count}/{total_rows} records so far."
#         )
#         update_data = []

# # Final leftover batch
# if update_data:
#     db.bulk_update_mappings(Pincode_Serviceability, update_data)
#     db.commit()
#     processed_count += len(update_data)
#     print(
#         f"✅ Final Update: {processed_count}/{total_rows} records updated."
#     )

# return GenericResponseModel(
#     status_code=http.HTTPStatus.OK,
#     message="Pincode serviceability updated successfully.",
#     status=True,
# )


#  df = pd.read_csv("order.csv")

#                 invalid_order_ids = []

#                 for index, row in df.iterrows():
#                     order_id_raw = str(row.get("Customer Reference Number", "")).strip()
#                     awb_code = row.get("CN #", "")

#                     if order_id_raw.startswith("LMI/"):
#                         try:
#                             parts = order_id_raw.split("/")
#                             if len(parts) != 3:
#                                 raise ValueError("Invalid LM format")

#                             _, client_id, order_id = parts

#                             client_id = int(client_id)
#                             order_id = str(order_id)
#                             awb_code = str(awb_code).strip()

#                             if client_id != 120:
#                                 continue

#                             order_id = order_id.split("|")[0]

#                             context_user_data.set(TempModel(**{"client_id": client_id}))

#                             # Replace this with your actual logic

#                             order = (
#                                 db.query(Order)
#                                 .filter(
#                                     Order.client_id == client_id,
#                                     Order.order_id.like(f"{order_id}%"),
#                                 )
#                                 .first()
#                             )

#                             if not order:
#                                 raise ValueError("Order not found in DB")

#                             if (
#                                 order.awb_number or order.status == "cancelled"
#                             ):  # Assuming awb_number is the field in your model

#                                 print(
#                                     f"AWB already assigned for order {order_id_raw}, skipping."
#                                 )
#                                 continue

#                             contract = (
#                                 db.query(Company_To_Client_Contract)
#                                 .join(Company_To_Client_Contract.aggregator_courier)
#                                 .filter(
#                                     Company_To_Client_Contract.client_id == client_id,
#                                     Aggregator_Courier.slug == "dtdc 3kg",
#                                 )
#                                 .options(
#                                     joinedload(
#                                         Company_To_Client_Contract.aggregator_courier
#                                     )
#                                 )
#                                 .first()
#                             )

#                             print(f"Contract found: {contract}")

#                             if not contract:
#                                 raise ValueError(
#                                     f"No contract found for client {client_id}"
#                                 )

#                             freight = ServiceabilityService.calculate_freight(
#                                 order_id=order_id,
#                                 min_chargeable_weight=contract.aggregator_courier.min_chargeable_weight,
#                                 additional_weight_bracket=contract.aggregator_courier.additional_weight_bracket,
#                                 contract_id=contract.id,
#                             )

#                             print(f"Freight calculated: {freight}")

#                             total_freight = (
#                                 freight["freight"]
#                                 + freight["cod_charges"]
#                                 + freight["tax_amount"]
#                             )

#                             print(f"Total freight: {total_freight}")

#                             WalletService.deduct_money(
#                                 total_freight, awb_number=awb_code
#                             )

#                             order.forward_freight = freight["freight"]
#                             order.forward_cod_charge = freight["cod_charges"]
#                             order.forward_tax = freight["tax_amount"]

#                             # Assign AWB and set status
#                             order.awb_number = awb_code
#                             order.status = "booked"
#                             order.sub_status = "booked"
#                             order.aggregator = "dtdc"
#                             order.courier_partner = "dtdc 3kg"

#                             db.add(order)
#                             db.commit()

#                             print(
#                                 f"Running logic for client_id={client_id}, order_id={order_id}, awb_code={awb_code}"
#                             )

#                         except Exception as e:
#                             print(f"Error processing {order_id_raw}: {e}")
#                             invalid_order_ids.append(order_id_raw)
#                     else:
#                         # You can define alternative processing or just log the invalid format
#                         print(f"Skipping non-LM format: {order_id_raw}")
#                         invalid_order_ids.append(order_id_raw)

#                 # Final output of problematic IDs
#                 if invalid_order_ids:
#                     print("\nInvalid Order IDs encountered:")
#                     for oid in invalid_order_ids:
#                         print(oid)

#                 return GenericResponseModel(
#                     status_code=http.HTTPStatus.OK,
#                     message="Order does not exist",
#                 )


#  orders = (
#                     db.query(Order)
#                     .filter(
#                         Order.status != "new",
#                         Order.status != "cancelled",
#                         Order.aggregator == "ats",
#                         Order.booking_date >= "2025-05-01 00:00:00.000 +0530",
#                         Order.status == "RTO",
#                     )
#                     .all()
#                 )

#                 # Load the ATS rates from the Excel file
#                 ats_rates = load_rate_chart("ats.xlsx", sheet_name="Standard")
#                 ats_rto_rates = load_rate_chart("ats.xlsx", sheet_name="RTO")

#                 # print(ats_rates)

#                 error_awbs = []

#                 count = 0
#                 for order in orders:

#                     try:
#                         count = count + 1
#                         print(count)
#                         status = order.status
#                         weight = order.applicable_weight
#                         zone = order.zone

#                         forward_rate = lookup_rate(ats_rates, weight, zone)
#                         reverse_rate = lookup_rate(ats_rto_rates, weight, zone)

#                         cod_charge = (
#                             0 if order.payment_mode.lower() == "prepaid" else 20
#                         )

#                         if status != "RTO":
#                             order.buy_forward_freight = forward_rate
#                             order.buy_forward_cod_charge = cod_charge
#                             order.buy_forward_tax = (
#                                 float(forward_rate + cod_charge) * 0.18
#                             )

#                         else:
#                             order.buy_forward_freight = forward_rate
#                             order.buy_forward_tax = float(forward_rate) * 0.18
#                             order.buy_forward_cod_charge = 0

#                             order.buy_rto_freight = reverse_rate
#                             order.buy_rto_tax = float(reverse_rate) * 0.18

#                         db.add(order)

#                     except Exception as e:
#                         error_awbs.append(order.awb_number)
#                         print(f"Error processing order {order.order_id}: {e}")
#                         continue

#                 db.commit()

#                 return GenericResponseModel(
#                     status_code=http.HTTPStatus.OK,
#                     message="Order does not exist",
#                 )


# TEMP LOGIC: Generate zone mapping Excel file
# source_pincode = 110075

# # Fetch pincodes from pincode_mapping joined with pincode_serviceability
# # Filter for pincodes where shadowfax_lm is true
# serviceable_pincodes = (
#     db.query(Pincode_Mapping)
#     .join(
#         Pincode_Serviceability,
#         Pincode_Mapping.pincode == Pincode_Serviceability.pincode,
#     )
#     .filter(Pincode_Serviceability.shadowfax_lm == True)
#     .all()
# )

# # Find source pincode record from serviceable pincodes
# source_pincode_record = None
# for pincode_record in serviceable_pincodes:
#     if pincode_record.pincode == source_pincode:
#         source_pincode_record = pincode_record
#         break

# # If source pincode is not in serviceable list, fetch it separately
# if not source_pincode_record:
#     source_pincode_record = (
#         db.query(Pincode_Mapping)
#         .filter(Pincode_Mapping.pincode == source_pincode)
#         .first()
#     )

# if not source_pincode_record:
#     logger.error(
#         extra=context_user_data.get(),
#         msg=f"Source pincode {source_pincode} not found in database",
#     )
#     return GenericResponseModel(
#         status_code=http.HTTPStatus.BAD_REQUEST,
#         message=f"Source pincode {source_pincode} not found",
#         status=False,
#     )

# logger.info(
#     extra=context_user_data.get(),
#     msg=f"Found {len(serviceable_pincodes)} serviceable pincodes with shadowfax_lm=true",
# )

# # Prepare data for Excel
# zone_mapping_data = []

# count = 0

# for destination_pincode_record in serviceable_pincodes:

#     count += 1
#     print(count)

#     destination_pincode = destination_pincode_record.pincode

#     # Calculate zone using optimized method with already fetched data
#     zone = OrderService.calculate_zone_optimized(
#         source_pincode_record, destination_pincode_record
#     )

#     zone_mapping_data.append(
#         {
#             "Source_Pincode": source_pincode,
#             "Destination_Pincode": destination_pincode,
#             "Destination_City": destination_pincode_record.city,
#             "Destination_State": destination_pincode_record.state,
#             "Zone": zone,
#         }
#     )

# # Create DataFrame and save to Excel
# df = pd.DataFrame(zone_mapping_data)

# # Create uploads directory if it doesn't exist
# uploads_dir = os.path.join(os.getcwd(), "uploads")
# os.makedirs(uploads_dir, exist_ok=True)

# # Generate filename with timestamp
# timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
# excel_filename = f"zone_mapping_{source_pincode}_{timestamp}.xlsx"
# excel_filepath = os.path.join(uploads_dir, excel_filename)

# # Save to Excel
# df.to_excel(excel_filepath, index=False)

# logger.info(
#     extra=context_user_data.get(),
#     msg=f"Zone mapping Excel file generated with {len(serviceable_pincodes)} serviceable pincodes: {excel_filepath}",
# )


# New Logic: Calculate freight for client_id = 93 orders
# print("=== Starting Freight Calculation for Client ID 93 ===")
# logger.info("Starting freight calculation for client ID 93")

# # Read the combined pincode mappings file
# combined_file_path = os.path.join(
#     os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
#     "combined_pincode_mappings.xlsx",
# )
# print(
#     f"📁 Reading combined pincode mappings from: {combined_file_path}"
# )

# try:
#     pincode_df = pd.read_excel(combined_file_path)
#     print(
#         f"✅ Combined file loaded successfully - Shape: {pincode_df.shape}"
#     )

#     # Pre-convert columns to string for fast lookups (one-time operation)
#     pincode_df["File_Name_str"] = pincode_df["File_Name"].astype(str)
#     pincode_df["Delivery_Pincode_str"] = pincode_df[
#         "Delivery Pincode"
#     ].astype(str)
#     print("✅ Pre-converted pincode columns to string for fast lookups")

#     logger.info(
#         f"Combined pincode mappings loaded - Shape: {pincode_df.shape}"
#     )
# except Exception as e:
#     print(f"❌ Error reading combined file: {str(e)}")
#     logger.error(f"Error reading combined pincode mappings: {str(e)}")
#     return GenericResponseModel(
#         status_code=http.HTTPStatus.BAD_REQUEST,
#         message=f"Error reading combined pincode mappings: {str(e)}",
#         status=False,
#     )

# # Import AWB list from data.py
# from .data import awbs

# print(f"📋 Loaded {len(awbs)} AWBs from data.py")
# logger.info(f"Loaded {len(awbs)} AWBs from data.py")

# # Get orders for client_id = 93 that are not in "new" or "cancelled" status and match AWB list
# print("\n🔍 Fetching orders for client_id = 93 with specific AWBs...")
# orders = (
#     db.query(Order)
#     .filter(
#         Order.client_id == 93,
#         ~Order.status.in_(
#             ["new", "cancelled"]
#         ),  # Exclude new and cancelled
#         Order.awb_number.in_(awbs),  # Filter by AWB list from data.py
#     )
#     .all()
# )

# print(f"📊 Found {len(orders)} orders matching the AWB list to process")
# logger.info(
#     f"Found {len(orders)} orders matching AWB list for freight calculation"
# )

# if not orders:
#     print("⚠️  No orders found matching the AWB list")
#     return GenericResponseModel(
#         status_code=http.HTTPStatus.OK,
#         message="No orders found for client_id 93 with the specified AWBs",
#         status=True,
#         data={"orders_processed": 0, "awb_count": len(awbs)},
#     )

# # Pre-load pickup locations to avoid N+1 queries
# pickup_location_codes = [
#     order.pickup_location_code
#     for order in orders
#     if order.pickup_location_code
# ]
# pickup_locations = {}
# if pickup_location_codes:
#     pickup_location_objects = (
#         db.query(Pickup_Location)
#         .filter(
#             Pickup_Location.location_code.in_(pickup_location_codes),
#             Pickup_Location.client_id == 93,
#         )
#         .all()
#     )
#     pickup_locations = {
#         loc.location_code: loc for loc in pickup_location_objects
#     }
#     print(f"🏢 Pre-loaded {len(pickup_locations)} pickup locations")

# freight_calculations = []
# processed_count = 0
# error_count = 0

# print("\n� Starting parallel batch processing...")

# # Prepare rate structure for batch processing
# rates = {
#     1: {  # Period 1: Sept 1, 2024 - April 1, 2025
#         "bluedart": {
#             "base": [35, 37, 38, 40.8, 65],
#             "additional": [34, 36, 37, 39.8, 64],
#             "cod_fixed": 32,
#             "cod_percentage": 1.5,
#         },
#         "bluedart-air": {
#             "base": [34, 36, 37, 39.8, 63],
#             "additional": [33, 35, 36, 38.8, 62],
#             "cod_fixed": 32,
#             "cod_percentage": 1.5,
#         },
#     },
#     2: {  # Period 2: April 1 - June 26, 2025
#         "bluedart": {
#             "base": [33, 36, 37, 39, 62],
#             "additional": [32, 35, 36, 38, 61],
#             "cod_fixed": 30,
#             "cod_percentage": 1.5,
#         },
#         "bluedart-air": {
#             "base": [37.4, 41.8, 49.5, 50.6, 72.6],
#             "additional": [36.3, 40.7, 48.4, 49.5, 71.5],
#             "cod_fixed": 30,
#             "cod_percentage": 1.5,
#         },
#     },
#     3: {  # Period 3: June 26, 2025 onwards
#         "bluedart": {
#             "base": [31, 33, 35, 37, 56],
#             "additional": [31, 33, 35, 37, 56],  # Same as base
#             "cod_fixed": 26,
#             "cod_percentage": 1.5,
#         },
#         "bluedart-air": {  # Same as period 2
#             "base": [37.4, 41.8, 49.5, 50.6, 72.6],
#             "additional": [36.3, 40.7, 48.4, 49.5, 71.5],
#             "cod_fixed": 30,
#             "cod_percentage": 1.5,
#         },
#     },
# }

# # Determine optimal batch size and number of processes
# num_processes = min(cpu_count(), 8)  # Use max 8 processes
# batch_size = max(
#     50, len(orders) // num_processes
# )  # At least 50 orders per batch

# print(f"📦 Batch Configuration:")
# print(f"   - Total orders: {len(orders)}")
# print(f"   - Processes: {num_processes}")
# print(f"   - Batch size: {batch_size}")

# # Create batches
# order_batches = []
# for i in range(0, len(orders), batch_size):
#     batch = orders[i : i + batch_size]
#     order_batches.append((batch, pincode_df, pickup_locations, rates))
#     print(
#         f"  Batch {len(order_batches)}: {len(batch)} orders (orders {i+1}-{min(i+batch_size, len(orders))})"
#     )

# print(f"   - Total batches: {len(order_batches)}")

# # Process batches in parallel
# with Pool(processes=num_processes) as pool:
#     batch_results = pool.map(
#         OrderService.process_order_batch, order_batches
#     )

# # Combine results from all batches
# print(f"\n📋 Processing results from {len(batch_results)} batches:")
# for batch_idx, (
#     batch_freight_calculations,
#     batch_error_count,
# ) in enumerate(batch_results, 1):
#     batch_success_count = len(batch_freight_calculations)
#     batch_total = batch_success_count + batch_error_count

#     print(
#         f"  Batch {batch_idx}: {batch_success_count} successful, {batch_error_count} errors, {batch_total} total"
#     )

#     freight_calculations.extend(batch_freight_calculations)
#     processed_count += batch_success_count
#     error_count += batch_error_count

# print(f"\n📊 Parallel Processing Summary:")
# print(f"  - Total orders found: {len(orders)}")
# print(f"  - Orders processed successfully: {processed_count}")
# print(f"  - Orders with errors: {error_count}")

# # Save freight calculations to Excel file
# if freight_calculations:
#     output_path = os.path.join(
#         os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
#         "freight_calculations.xlsx",
#     )
#     print(f"\n💾 Saving freight calculations to: {output_path}")

#     freight_df = pd.DataFrame(freight_calculations)
#     freight_df.to_excel(output_path, index=False)
#     print("✅ Freight calculations saved successfully!")

#     logger.info(f"Freight calculations saved to {output_path}")

#     success_message = f"Freight calculation completed! Processed {processed_count} orders successfully."
#     print(f"\n🎉 {success_message}")

#     return GenericResponseModel(
#         status_code=http.HTTPStatus.OK,
#         message=success_message,
#         status=True,
#         data={
#             "output_file": output_path,
#             "total_orders": len(orders),
#             "orders_processed": processed_count,
#             "orders_with_errors": error_count,
#             "freight_calculations": freight_calculations[
#                 :10
#             ],  # Return first 10 for preview
#         },
#     )
# else:
#     print("❌ No freight calculations generated")
#     return GenericResponseModel(
#         status_code=http.HTTPStatus.BAD_REQUEST,
#         message="No freight calculations could be generated",
#         status=False,
#     )


# @staticmethod
#     def process_order_batch(batch_data):
#         """
#         Process a batch of orders for freight calculation
#         batch_data: tuple containing (order_batch, pincode_df, pickup_locations, rates)
#         """
#         order_batch, pincode_df, pickup_locations, rates = batch_data
#         batch_results = []
#         batch_errors = 0

#         for order in order_batch:
#             try:

#                 source_mapping = {
#                     "110018": "110039",
#                     "110019": "110039",
#                     "110031": "110039",
#                     "110033": "110039",
#                     "110036": "110039",
#                     "110039": "110039",
#                     "110041": "110039",
#                     "110043": "110039",
#                     "110048": "110039",
#                     "110085": "110039",
#                     "110087": "110039",
#                     "110091": "110039",
#                     "122003": "122018",
#                     "201206": "201010",
#                 }

#                 # Check if order has required data
#                 if not order.awb_number:
#                     batch_errors += 1
#                     continue

#                 # Determine if this is an RTO order based on status
#                 rto_statuses = [
#                     "RTO",
#                     "RTO-Delivered",
#                     "RTO-Out for Pickup",
#                     "RTO-Out for delivery",
#                     "RTO-Pickup Rescheduled",
#                     "RTO-Undelivered",
#                 ]
#                 is_rto = order.status in rto_statuses

#                 # Get pickup location
#                 pickup_location = pickup_locations.get(order.pickup_location_code)
#                 if not pickup_location:
#                     batch_errors += 1
#                     continue

#                 source_pincode = pickup_location.pincode
#                 destination_pincode = order.consignee_pincode
#                 zone = order.zone

#                 # Skip if courier_partner is not bluedart or bluedart-air
#                 if order.courier_partner not in ["bluedart", "bluedart-air"]:
#                     batch_errors += 1
#                     continue

#                 # Get zone from combined pincode mappings
#                 try:
#                     # Step 1: Try with original source pincode
#                     zone_lookup = pincode_df[
#                         (pincode_df["File_Name_str"] == str(source_pincode))
#                         & (
#                             pincode_df["Delivery_Pincode_str"]
#                             == str(destination_pincode)
#                         )
#                     ]

#                     zone_found_in_sheet = False
#                     mapped_source_used = False

#                     if zone_lookup is None or zone_lookup.empty:
#                         # Step 2: Try with mapped source pincode if available
#                         mapped_source = source_mapping.get(str(source_pincode))
#                         if mapped_source:
#                             zone_lookup = pincode_df[
#                                 (pincode_df["File_Name_str"] == str(mapped_source))
#                                 & (
#                                     pincode_df["Delivery_Pincode_str"]
#                                     == str(destination_pincode)
#                                 )
#                             ]
#                             if not zone_lookup.empty:
#                                 zone_found_in_sheet = True
#                                 mapped_source_used = True
#                                 raw_zone = zone_lookup.iloc[0]["Zone"]
#                                 # Format zone from "z_a" to "A"
#                                 if str(raw_zone).startswith("z_"):
#                                     zone = str(raw_zone)[
#                                         2:
#                                     ].upper()  # Remove "z_" and convert to uppercase
#                                 else:
#                                     zone = str(raw_zone).upper()

#                     else:
#                         # Step 1 succeeded - found with original source
#                         zone_found_in_sheet = True
#                         raw_zone = zone_lookup.iloc[0]["Zone"]
#                         # Format zone from "z_a" to "A"
#                         if str(raw_zone).startswith("z_"):
#                             zone = str(raw_zone)[
#                                 2:
#                             ].upper()  # Remove "z_" and convert to uppercase
#                         else:
#                             zone = str(raw_zone).upper()

#                     # Step 3: Fall back to order zone or default
#                     if not zone_found_in_sheet:
#                         if not zone:
#                             zone = "D"  # Default to zone D if no zone found

#                 except Exception as zone_error:
#                     zone_found_in_sheet = False
#                     mapped_source_used = False
#                     if not zone:
#                         zone = "D"

#                 # Determine rate period based on booking date
#                 booking_date = order.booking_date
#                 if not booking_date:
#                     booking_date = (
#                         order.created_at
#                     )  # Fallback to created_at if no booking_date

#                 # Convert booking_date to naive datetime for comparison
#                 if (
#                     booking_date
#                     and hasattr(booking_date, "tzinfo")
#                     and booking_date.tzinfo is not None
#                 ):
#                     booking_date_naive = booking_date.replace(tzinfo=None)
#                 else:
#                     booking_date_naive = booking_date

#                 # Determine rate period
#                 rate_period = 1  # Default
#                 rate_period_text = "Period 1: Sept 1, 2024 - April 1, 2025"

#                 if booking_date_naive:
#                     if booking_date_naive >= datetime(2025, 6, 26):
#                         rate_period = 3
#                         rate_period_text = "Period 3: June 26, 2025 onwards"
#                     elif booking_date_naive >= datetime(2025, 4, 1):
#                         rate_period = 2
#                         rate_period_text = "Period 2: April 1 - June 26, 2025"

#                 # Determine zone index (A=0, B=1, C=2, D=3, E=4)
#                 zone_mapping = {"A": 0, "B": 1, "C": 2, "D": 3, "E": 4}
#                 idx = zone_mapping.get(zone.upper(), 3)  # Default to D if unknown

#                 # Get applicable rates
#                 courier_rates = rates[rate_period][order.courier_partner]
#                 base_rate = courier_rates["base"][idx]
#                 additional_rate = courier_rates["additional"][idx]
#                 cod_fixed = courier_rates["cod_fixed"]
#                 cod_percentage = courier_rates["cod_percentage"]

#                 # Calculate freight manually
#                 applicable_weight = float(
#                     order.applicable_weight or order.weight or 0.5
#                 )
#                 min_chargeable_weight = 0.5  # Assuming 500g minimum
#                 additional_weight_bracket = 0.5  # Assuming 500g brackets

#                 if applicable_weight < min_chargeable_weight:
#                     applicable_weight = min_chargeable_weight

#                 # Calculate chargeable weight
#                 if applicable_weight > min_chargeable_weight:
#                     additional_weight = applicable_weight - min_chargeable_weight
#                     additional_bracket_count = math.ceil(
#                         additional_weight / additional_weight_bracket
#                     )
#                     chargeable_weight = min_chargeable_weight + (
#                         additional_weight_bracket * additional_bracket_count
#                     )
#                 else:
#                     additional_bracket_count = 0
#                     chargeable_weight = min_chargeable_weight

#                 # Calculate freight
#                 base_freight = float(base_rate)
#                 additional_freight = float(additional_rate) * additional_bracket_count
#                 forward_freight = base_freight + additional_freight

#                 # Calculate COD charges
#                 cod_charges = 0
#                 if (
#                     order.payment_mode
#                     and order.payment_mode.lower() == "cod"
#                     and not is_rto
#                 ):
#                     order_value = float(order.order_value or order.total_amount or 0)
#                     cod_charges = max(
#                         float(cod_fixed), (order_value * float(cod_percentage) / 100)
#                     )

#                 # Calculate RTO freight if applicable
#                 rto_freight = 0
#                 if is_rto:
#                     # RTO uses same rates as forward
#                     rto_freight = forward_freight
#                     cod_charges = 0  # No COD charges for RTO

#                 total_freight = forward_freight + rto_freight + cod_charges
#                 tax_amount = float(total_freight) * 0.18

#                 freight_calculation = {
#                     "order_id": order.order_id,
#                     "awb_number": order.awb_number,
#                     "status": order.status,
#                     "courier_partner": order.courier_partner,
#                     "booking_date": (
#                         booking_date_naive.strftime("%Y-%m-%d")
#                         if booking_date_naive
#                         else ""
#                     ),
#                     "rate_period": rate_period_text,
#                     "is_rto": is_rto,
#                     "source_pincode": source_pincode,
#                     "destination_pincode": destination_pincode,
#                     "zone": zone.upper(),
#                     "zone_found_in_sheet": zone_found_in_sheet,
#                     "mapped_source_used": mapped_source_used,
#                     "zone_source": (
#                         "Pincode Mapping Sheet (Mapped Source)"
#                         if zone_found_in_sheet and mapped_source_used
#                         else (
#                             "Pincode Mapping Sheet (Original Source)"
#                             if zone_found_in_sheet and not mapped_source_used
#                             else "Order Zone/Default"
#                         )
#                     ),
#                     "applicable_weight": float(applicable_weight),
#                     "chargeable_weight": float(chargeable_weight),
#                     "base_rate": round(float(base_rate), 2),
#                     "additional_rate": round(float(additional_rate), 2),
#                     "forward_freight": round(float(forward_freight), 2),
#                     "rto_freight": round(float(rto_freight), 2),
#                     "total_freight": round(float(total_freight), 2),
#                     "cod_charges": round(float(cod_charges), 2),
#                     "tax_amount": round(float(tax_amount), 2),
#                     "grand_total": round(float(total_freight + tax_amount), 2),
#                 }

#                 batch_results.append(freight_calculation)

#             except Exception as order_error:
#                 batch_errors += 1
#                 continue

#         return batch_results, batch_errors


# Special adhoc logic for zone mapping - trigger with order_id = "ZONE_MAPPING"
# if order_id == "ZONE_MAPPING":
# import os
# import pandas as pd
# from openpyxl import Workbook

# # Read the CSV file
# csv_file_path = os.path.join(
#     os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
#     "pincode_serviceability.csv",
# )

# if os.path.exists(csv_file_path):
#     try:
#         # Read CSV file
#         df = pd.read_csv(csv_file_path)

#         # Check if required columns exist
#         if "pincode" in df.columns:
#             source_pincode = "641101"

#             # Create zone mapping based on delivery pincodes
#             zone_mapping = []

#             # Get source pincode mapping record for zone calculation
#             source_pincode_record = (
#                 db.query(Pincode_Mapping)
#                 .filter(Pincode_Mapping.pincode == int(source_pincode))
#                 .first()
#             )

#             # Extract all delivery pincodes from DataFrame
#             all_delivery_pincodes = [
#                 int(row["pincode"]) for index, row in df.iterrows()
#             ]

#             # Fetch all pincode mapping records in a single query to avoid N+1 problem
#             pincode_mapping_records = (
#                 db.query(Pincode_Mapping)
#                 .filter(
#                     Pincode_Mapping.pincode.in_(all_delivery_pincodes)
#                 )
#                 .all()
#             )

#             # Create a dictionary for quick lookup
#             pincode_mapping_dict = {
#                 record.pincode: record
#                 for record in pincode_mapping_records
#             }

#             for index, row in df.iterrows():
#                 delivery_pincode = str(row["pincode"])

#                 print(delivery_pincode, ">>delivery_pincode<<")

#                 print(
#                     len(pincode_mapping_records),
#                     ">>pincode_mapping_records<<",
#                 )

#                 service_prepaid = str(row["ekart_lm_prepaid"])
#                 service_cod = str(row["ekart_lm_cod"])

#                 print(
#                     service_cod,
#                     service_prepaid,
#                     ">>service_cod, service_prepaid<<",
#                 )

#                 if service_prepaid != "True" and service_cod != "True":
#                     continue

#                 # Get destination pincode mapping record from pre-fetched dictionary
#                 destination_pincode_record = pincode_mapping_dict.get(
#                     int(delivery_pincode)
#                 )

#                 # Initialize zone_calculation_method variable
#                 zone_calculation_method = "Fallback"

#                 if destination_pincode_record:
#                     print(
#                         destination_pincode_record.pincode,
#                         ">>destination_pincode_record<<",
#                     )

#                 # Use serviceability zone calculation logic
#                 if source_pincode_record and destination_pincode_record:
#                     calculated_zone = (
#                         OrderService.calculate_zone_optimized(
#                             source_pincode_record,
#                             destination_pincode_record,
#                         )
#                     )
#                     zone_calculation_method = "Serviceability Logic"

#                 else:
#                     # Fallback to simple calculation if pincode mapping not found
#                     calculated_zone = "D"
#                     zone_calculation_method = "Fallback"

#                 print(calculated_zone)

#                 zone_mapping.append(
#                     {
#                         "Source_Pincode": source_pincode,
#                         "Delivery_Pincode": delivery_pincode,
#                         "Courier": "Bluedart",
#                         "Calculated_Zone": calculated_zone,
#                         "Zone_Calculation_Method": zone_calculation_method,
#                     }
#                 )

#             # Create DataFrame from zone mapping
#             zone_df = pd.DataFrame(zone_mapping)

#             # Save to Excel file
#             excel_file_path = os.path.join(
#                 os.path.dirname(
#                     os.path.dirname(os.path.dirname(__file__))
#                 ),
#                 "zone_mapping_302020.xlsx",
#             )

#             with pd.ExcelWriter(
#                 excel_file_path, engine="openpyxl"
#             ) as writer:
#                 zone_df.to_excel(
#                     writer, sheet_name="Zone_Mapping", index=False
#                 )

#                 # Add summary sheet
#                 summary_data = zone_df.groupby("Calculated_Zone").agg(
#                     {"Delivery_Pincode": "count"}
#                 )
#                 summary_data.columns = ["Count"]
#                 summary_data.to_excel(writer, sheet_name="Zone_Summary")

#                 # Add method breakdown summary
#                 method_summary = (
#                     zone_df.groupby(
#                         ["Zone_Calculation_Method", "Calculated_Zone"]
#                     )
#                     .size()
#                     .unstack(fill_value=0)
#                 )
#                 method_summary.to_excel(
#                     writer, sheet_name="Method_Summary"
#                 )

#             logger.info(
#                 f"Zone mapping created successfully. Total records: {len(zone_mapping)}"
#             )
#             logger.info(f"Excel file saved at: {excel_file_path}")

#             return GenericResponseModel(
#                 status_code=http.HTTPStatus.OK,
#                 message=f"Zone mapping created successfully. {len(zone_mapping)} records processed and saved to Excel.",
#                 status=True,
#                 data={
#                     "total_records": len(zone_mapping),
#                     "excel_file_path": excel_file_path,
#                     "zone_distribution": zone_df["Calculated_Zone"]
#                     .value_counts()
#                     .to_dict(),
#                 },
#             )
#         else:
#             return GenericResponseModel(
#                 status_code=http.HTTPStatus.BAD_REQUEST,
#                 message="'Delivery Pincode' column not found in CSV file",
#                 status=False,
#             )

#     except Exception as csv_error:
#         logger.error(f"Error processing CSV file: {str(csv_error)}")
#         return GenericResponseModel(
#             status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
#             message=f"Error processing CSV file: {str(csv_error)}",
#             status=False,
#         )
# else:
#     return GenericResponseModel(
#         status_code=http.HTTPStatus.NOT_FOUND,
#         message="CSV file RTO.csv not found in root directory",
#         status=False,
#     )


def upload_pincode_master(excel_file_path: str, update_existing: bool = True):
    """
    Upload pincode master data from Excel file to pincode_mapping table.
    
    Args:
        excel_file_path: Path to the Excel file with headers: Pincode, State, City
        update_existing: If True, updates existing records. If False, skips duplicates.
    
    Returns:
        dict: Summary with counts of inserted, updated, skipped, and error records
    """
    import pandas as pd
    import os
    from database.db import SessionLocal
    from models.pincode_mapping import Pincode_Mapping
    from logger import logger
    
    if not os.path.exists(excel_file_path):
        raise FileNotFoundError(f"Excel file not found: {excel_file_path}")
    
    # Read Excel file
    try:
        df = pd.read_excel(excel_file_path)
    except Exception as e:
        raise ValueError(f"Error reading Excel file: {str(e)}")
    
    # Normalize column names (case-insensitive, strip whitespace)
    df.columns = df.columns.str.strip()
    expected_columns = ['Pincode', 'State', 'City']
    
    # Check if required columns exist (case-insensitive)
    df_columns_lower = [col.lower() for col in df.columns]
    missing_columns = []
    for col in expected_columns:
        if col.lower() not in df_columns_lower:
            missing_columns.append(col)
    
    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}. "
            f"Found columns: {list(df.columns)}"
        )
    
    # Normalize column names to match expected format
    column_mapping = {}
    for col in df.columns:
        if col.lower() == 'pincode':
            column_mapping[col] = 'Pincode'
        elif col.lower() == 'state':
            column_mapping[col] = 'State'
        elif col.lower() == 'city':
            column_mapping[col] = 'City'
    
    df = df.rename(columns=column_mapping)
    
    # Select only required columns
    df = df[['Pincode', 'State', 'City']].copy()
    
    # Remove rows with missing pincode
    df = df.dropna(subset=['Pincode'])
    
    # Convert pincode to integer (handle any string pincodes)
    try:
        df['Pincode'] = df['Pincode'].astype(int)
    except ValueError as e:
        raise ValueError(f"Invalid pincode values found. All pincodes must be numeric: {str(e)}")
    
    # Clean and validate data
    df['State'] = df['State'].astype(str).str.strip()
    df['City'] = df['City'].astype(str).str.strip()
    
    # Remove rows with empty state or city
    df = df[(df['State'] != '') & (df['City'] != '') & (df['State'] != 'nan') & (df['City'] != 'nan')]
    
    # Normalize to lowercase for consistency (all comparisons use .lower() in codebase)
    df['State'] = df['State'].str.lower()
    df['City'] = df['City'].str.lower()
    
    # Truncate state and city to 50 characters (matching DB schema)
    df['State'] = df['State'].str[:50]
    df['City'] = df['City'].str[:50]
    
    # Sort by pincode ascending before processing
    df = df.sort_values(by='Pincode', ascending=True).reset_index(drop=True)
    
    # Get database session
    db = SessionLocal()
    
    try:
        inserted_count = 0
        updated_count = 0
        skipped_count = 0
        error_count = 0
        errors = []
        
        # Process in batches for better performance
        batch_size = 1000
        total_rows = len(df)
        
        logger.info(f"Starting pincode master upload. Total rows to process: {total_rows}")
        
        for i in range(0, total_rows, batch_size):
            batch_df = df.iloc[i:i+batch_size]
            batch_records = []
            
            for _, row in batch_df.iterrows():
                try:
                    pincode = int(row['Pincode'])
                    # Values are already normalized to lowercase in dataframe processing above
                    state = str(row['State'])
                    city = str(row['City'])
                    
                    # Check if record already exists
                    existing = db.query(Pincode_Mapping).filter(
                        Pincode_Mapping.pincode == pincode
                    ).first()
                    
                    if existing:
                        if update_existing:
                            # Update existing record
                            existing.state = state
                            existing.city = city
                            updated_count += 1
                        else:
                            # Skip duplicate
                            skipped_count += 1
                    else:
                        # Prepare new record for bulk insert
                        batch_records.append({
                            'pincode': pincode,
                            'state': state,
                            'city': city
                        })
                        
                except Exception as e:
                    error_count += 1
                    errors.append({
                        'pincode': row.get('Pincode', 'N/A'),
                        'error': str(e)
                    })
                    logger.error(f"Error processing row with pincode {row.get('Pincode', 'N/A')}: {str(e)}")
                    continue
            
            # Bulk insert new records
            if batch_records:
                try:
                    db.bulk_insert_mappings(Pincode_Mapping, batch_records)
                    inserted_count += len(batch_records)
                    logger.info(f"Inserted batch: {len(batch_records)} records (Total inserted so far: {inserted_count})")
                except Exception as e:
                    error_count += len(batch_records)
                    logger.error(f"Error bulk inserting batch: {str(e)}")
                    for record in batch_records:
                        errors.append({
                            'pincode': record.get('pincode', 'N/A'),
                            'error': str(e)
                        })
            
            # Commit after each batch
            db.commit()
            logger.info(f"Processed batch {i//batch_size + 1}/{(total_rows-1)//batch_size + 1}")
        
        summary = {
            'total_rows_in_file': total_rows,
            'inserted': inserted_count,
            'updated': updated_count,
            'skipped': skipped_count,
            'errors': error_count,
            'error_details': errors[:10] if errors else []  # Show first 10 errors
        }
        
        logger.info(f"Pincode master upload completed. Summary: {summary}")
        
        return summary
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error during pincode master upload: {str(e)}")
        raise
    finally:
        db.close()