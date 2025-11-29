# Pincode_Mapping Table Optimization Analysis

## Current State Analysis

### Query Patterns Found:
1. **Single Pincode Lookups** (Most Common):
   - `Pincode_Mapping.pincode == pincode` 
   - Used in: `shipment_service.py`, `serviceability_service.py`
   - Frequency: High (per order/shipment lookup)

2. **Bulk Pincode Lookups**:
   - `Pincode_Mapping.pincode.in_(chunk_pincodes)` 
   - Used in: `order_service.py` (batches of 1000+ pincodes)
   - Frequency: Medium (batch processing)

3. **Selected Columns**:
   - Always selects: `pincode`, `city`, `state`
   - Never filters by city or state alone

### Current Issues:
- ❌ **No index on pincode column** → Full table scans on every query
- ❌ **No uniqueness constraint** → Potential duplicate pincodes
- ❌ **No covering index** → Extra table lookups even when pincode is found

## Optimizations Applied

### 1. Unique Constraint on Pincode
```python
pincode = Column(Integer, nullable=False, unique=True)
```
**Benefits:**
- Ensures data integrity (one pincode = one city/state mapping)
- Automatically creates a unique B-tree index
- Prevents accidental duplicates during bulk uploads

**Performance Impact:**
- Single pincode lookups: **O(log n)** instead of **O(n)** (table scan)
- Bulk lookups with `.in_()`: **Much faster** with index

### 2. Composite Covering Index
```python
Index("ix_pincode_mapping_pincode_city_state", "pincode", "city", "state")
```
**Benefits:**
- **Index-only scans**: Database can satisfy queries without accessing table data
- All queried columns (`pincode`, `city`, `state`) are in the index
- Especially beneficial for bulk queries

**Performance Impact:**
- Reduces I/O operations
- Faster bulk lookups (1000+ pincodes at once)

## Expected Performance Improvements

### Before Optimization:
- Single lookup: **~50-200ms** (table scan on large dataset)
- Bulk lookup (1000 pincodes): **~500-2000ms**

### After Optimization:
- Single lookup: **~1-5ms** (index seek)
- Bulk lookup (1000 pincodes): **~10-50ms** (index scan)

**Estimated improvement: 10-100x faster** depending on table size

## Migration Steps

1. **Check for duplicates** (if any exist, clean them first):
   ```sql
   SELECT pincode, COUNT(*) 
   FROM pincode_mapping 
   WHERE is_deleted = false
   GROUP BY pincode 
   HAVING COUNT(*) > 1;
   ```

2. **Run migration script**:
   ```bash
   psql -d your_database -f scripts/migrate_pincode_mapping_indexes.sql
   ```

3. **Verify indexes created**:
   ```sql
   SELECT indexname, indexdef 
   FROM pg_indexes 
   WHERE tablename = 'pincode_mapping';
   ```

## Notes

- The unique constraint will fail if duplicates exist - clean them first
- Index creation may take a few minutes on large tables (100k+ rows)
- The covering index is slightly larger but provides significant performance gains
- Both indexes are automatically maintained by PostgreSQL

## Comparison with Similar Tables

Looking at `pickup_location` model:
- Has `index=True` on pincode column ✅
- Has composite indexes for common query patterns ✅
- `pincode_mapping` should follow the same pattern ✅

