 excel_file = Path("modules/orders/weight.xlsx")

                df = pd.read_excel(excel_file, dtype=str, keep_default_na=False)
                orders = df.to_dict(orient="records")

                print("heell weight update mapping")

                for index, data in enumerate(orders):

                    try:
                        # print(data.get("AWB"))

                        freight = data.get("freight")

                        print(freight)

                        if freight is not None and freight.strip() != "":
                            continue

                        print("freight is not present")

                        order = (
                            db.query(Order)
                            .filter(Order.awb_number == data.get("AWB"))
                            .first()
                        )

                        context_user_data.set(
                            TempModel(**{"client_id": order.client_id})
                        )

                        if order is None:
                            continue

                        print(order.order_id)

                        contract = (
                            db.query(Company_To_Client_Contract)
                            .join(Company_To_Client_Contract.aggregator_courier)
                            .filter(
                                Company_To_Client_Contract.client_id == order.client_id,
                                Company_To_Client_Contract.isActive == True,
                                Aggregator_Courier.slug == order.courier_partner,
                            )
                            .options(
                                joinedload(
                                    Company_To_Client_Contract.aggregator_courier
                                )
                            )
                            .first()
                        )

                        print("contract - ", contract.id)

                        freight = ServiceabilityService.calculate_freight(
                            order_id=order.order_id,
                            min_chargeable_weight=contract.aggregator_courier.min_chargeable_weight,
                            additional_weight_bracket=contract.aggregator_courier.additional_weight_bracket,
                            contract_id=contract.id,
                            weight=float(data.get("3PL Weight")),
                        )

                        print(freight)

                        df.at[index, "lm_zone"] = order.zone
                        df.at[index, "freight"] = freight.get("freight")
                        df.at[index, "cod_charge"] = freight.get("cod_charges")
                        df.at[index, "tax"] = freight.get("tax_amount")
                        df.at[index, "chargeable_weight"] = freight.get(
                            "chargeable_weight"
                        )

                        print(order.status)

                        if order.status == "RTO":
                            rto_freight = ServiceabilityService.calculate_rto_freight(
                                order_id=order.order_id,
                                min_chargeable_weight=contract.aggregator_courier.min_chargeable_weight,
                                additional_weight_bracket=contract.aggregator_courier.additional_weight_bracket,
                                contract_id=contract.id,
                                weight=float(data.get("3PL Weight")),
                            )

                            print(rto_freight)

                            df.at[index, "rto_freight"] = rto_freight.get("rto_freight")
                            df.at[index, "rto_tax"] = rto_freight.get("rto_tax")
                            df.at[index, "cod_charge"] = 0

                    except:
                        continue

                output_file = Path("modules/orders/updated_weight.xlsx")
                df.to_excel(output_file, index=False)

                return