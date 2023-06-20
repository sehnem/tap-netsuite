REPLICATION_KEYS = ["lastmodifieddate", "lastmoddate"]

RETRYABLE_ERRORS = [
    "ACCT_TEMP_UNAVAILABLE",
    "BILL_PAY_STATUS_UNAVAILABLE",
    "BILLPAY_SRVC_UNAVAILBL",
    "PAYROLL_IN_PROCESS",
]

SEARCH_ONLY_FIELDS = ["AccountingTransaction", "Item", "Transaction"]

CUSTOM_SEARCH_FIELDS = {
    "TransactionSearchBasic": [
        "AssemblyBuild",
        "AssemblyUnbuild",
        "BinTransfer",
        "BinWorksheet",
        "CashRefund",
        "CashSale",
        "Check",
        "CreditMemo",
        "CustomerDeposit",
        "CustomerPayment",
        "CustomerRefund",
        "CustomPurchase",
        "CustomSale",
        "Deposit",
        "DepositApplication",
        "Estimate",
        "ExpenseReport",
        "InterCompanyJournalEntry",
        "InventoryAdjustment",
        "InventoryCostRevaluation",
        "InventoryTransfer",
        "Invoice",
        "ItemFulfillment",
        "ItemReceipt",
        "JournalEntry",
        "Opportunity",
        "PaycheckJournal",
        "PurchaseOrder",
        "ReturnAuthorization",
        "SalesOrder",
        "StatisticalJournalEntry",
        "TransferOrder",
        "VendorBill",
        "VendorCredit",
        "VendorPayment",
        "VendorReturnAuthorization",
        "WorkOrder",
        "WorkOrderClose",
        "WorkOrderCompletion",
        "WorkOrderIssue",
    ],
    "ItemSearchBasic": ["InventoryItem"],
}


CUSTOM_FIELD_TYPES =  [
    "crmCustomField",
    "customList",
    "customRecordCustomField",
    "customSegment",
    "entityCustomField",
    "itemCustomField",
    "itemOptionCustomField",
    "otherCustomField",
    "transactionBodyCustomField",
    "transactionColumnCustomField"
]



CUSTOM_FIELD_SCHEMA =  {"anyOf": [
                                {
                                    "type":["array","null"],
                                    "items": { 
                                        "properties": { 
                                            "internalId": { 
                                                "type":["string","null"]
                                            },
                                            "externalId": { 
                                                "type":["string","null"]
                                            },
                                            "name": { 
                                                "type":["string","null"]
                                            },
                                            "typeId": { 
                                                "type":["string","null"]
                                            }
                                        },
                                        "type":"object"
                                    },
                                },
                                {
                                    "type":"object",
                                     "properties": { 
                                            "internalId": { 
                                                "type":["string","null"]
                                            },
                                            "externalId": { 
                                                "type":["string","null"]
                                            },
                                            "name": { 
                                                "type":["string","null"]
                                            },
                                            "typeId": { 
                                                "type":["string","null"]
                                            }
                                    }
                                }, 
                                {
                                    "type":["string","boolean","integer","number"]
                                }

                            ]
                        }