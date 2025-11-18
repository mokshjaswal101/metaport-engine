status_mapping = {
    "FORWARD": {
        "PickupDone": {"status": "in transit", "sub_status": "pickup completed"},
        "RecipientRequestedAlternateDeliveryTiming": {
            "status": "NDR",
            "sub_status": "NDR",
        },
        "ReadyForReceive": {"status": "pickup", "sub_status": "pickup pending"},
        "Delivered": {"status": "delivered", "sub_status": "delivered"},
        "Departed": {"status": "in transit", "sub_status": "in transit"},
        "DeliveryAttempted": {"status": "NDR", "sub_status": "NDR"},
        "Lost": {"status": "NDR", "sub_status": "lost"},
        "OutForDelivery": {
            "status": "out for delivery",
            "sub_status": "out for delivery",
        },
        "ArrivedAtCarrierFacility": {
            "status": "in transit",
            "sub_status": "in transit",
        },
        "Rejected": {"status": "NDR", "sub_status": "NDR"},
        "Undeliverable": {"status": "NDR", "sub_status": "NDR"},
        "PickupCancelled": {"status": "pickup", "sub_status": "pickup failed"},
        "ReturnInitiated": {"status": "RTO", "sub_status": "RTO initiated"},
        "AvailableForPickup": {"status": "pickup", "sub_status": "pickup pending"},
    },
    "RETURNS": {
        "Delivered": {"status": "RTO", "sub_status": "RTO delivered"},
        "Departed": {"status": "RTO", "sub_status": "RTO in transit"},
        "DeliveryAttempted": {"status": "RTO", "sub_status": "RTO in transit"},
        "OutForDelivery": {
            "status": "RTO",
            "sub_status": "RTO out for delivery",
        },
        "ArrivedAtCarrierFacility": {"status": "RTO", "sub_status": "RTO in transit"},
        "ReturnInitiated": {"status": "RTO", "sub_status": "RTO initiated"},
        "Undeliverable": {"status": "RTO", "sub_status": "RTO NDR"},
        "Returned": {"status": "RTO", "sub_status": "RTO delivered"},
    },
}
