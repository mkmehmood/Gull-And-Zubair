import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';

let deletedRecordIds = new Set();
let deletionRecords  = [];

const _UUID_RE = /^[a-z0-9][a-z0-9_-]*-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const _STD_UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

let _seq = 0;
function generateUUID(prefix = '') {
  const hex = (n, w) => n.toString(16).padStart(w, '0');
  const now = Date.now();
  const rand = Math.floor(Math.random() * 0xfff);
  const uuid = `${hex(now >>> 16, 8)}-${hex(now & 0xffff, 4)}-4${hex(rand, 3)}-${hex(0x80 | ((_seq++ & 0x3f)), 2)}${hex(Math.floor(Math.random() * 0xff), 2)}-${hex(Math.floor(Math.random() * 0xffffffffffff), 12)}`;
  return prefix ? `${prefix}-${uuid}` : uuid;
}

function validateUUID(uuid) {
  if (!uuid || typeof uuid !== 'string') return false;
  return _STD_UUID.test(uuid) || _UUID_RE.test(uuid);
}

function makeSqliteStore() {
  const store = new Map();
  return {
    async get(key, def) {
      return store.has(key) ? JSON.parse(JSON.stringify(store.get(key))) : def;
    },
    async set(key, value) {
      store.set(key, JSON.parse(JSON.stringify(value)));
    },
    async remove(key) {
      store.delete(key);
    },
    _raw: store,
  };
}

function makeOfflineQueue() {
  return {
    queue: [],
    deadLetterQueue: [],
    async saveQueue() {},
    async saveDeadLetterQueue() {},
    async add(op) { this.queue.push({ operation: op }); },
  };
}

function _safeErr(e) { return e?.message ?? String(e); }

function purgeRecoveredId(id, collectionName, cleanRecord, newId, { sqliteStore, OfflineQueue, firebaseDB, currentUser }) {
  const sid    = String(id);
  const newSid = newId ? String(newId) : sid;

  deletedRecordIds.delete(sid);
  if (Array.isArray(deletionRecords)) {
    deletionRecords = deletionRecords.filter(r => r.id !== sid && r.recordId !== sid);
  }

  const purgeLocal = async () => {
    try {
      const fresh  = await sqliteStore.get('deletion_records', []);
      const pruned = Array.isArray(fresh) ? fresh.filter(r => r.id !== sid && r.recordId !== sid) : [];
      await sqliteStore.set('deletion_records', pruned);
    } catch(e) { console.warn('[RecycleBin] purge SQLite deletion_records failed:', _safeErr(e)); }
    try {
      await sqliteStore.set('deleted_records', Array.from(deletedRecordIds));
    } catch(e) { console.warn('[RecycleBin] purge SQLite deleted_records failed:', _safeErr(e)); }

    if (typeof OfflineQueue !== 'undefined' && OfflineQueue !== null) {
      const _isStale = (item) => {
        const op = item.operation || {};
        return (
          (op.action === 'delete' && op.docId === sid) ||
          (op.action === 'set'    && op.docId === sid && (op.data === null || op.data === undefined))
        );
      };
      const qBefore = OfflineQueue.queue.length;
      OfflineQueue.queue = OfflineQueue.queue.filter(i => !_isStale(i));
      if (OfflineQueue.queue.length !== qBefore) {
        try { await OfflineQueue.saveQueue(); } catch(e) {}
      }
      const dlBefore = (OfflineQueue.deadLetterQueue || []).length;
      if (Array.isArray(OfflineQueue.deadLetterQueue)) {
        OfflineQueue.deadLetterQueue = OfflineQueue.deadLetterQueue.filter(i => !_isStale(i));
        if (OfflineQueue.deadLetterQueue.length !== dlBefore) {
          try { await OfflineQueue.saveDeadLetterQueue(); } catch(e) {}
        }
      }
    }
  };

  return purgeLocal();
}

async function recoverRecord(deletedId, collectionName, { sqliteStore, OfflineQueue, firebaseDB = null, currentUser = null }) {
  if (!deletedId || !collectionName) return false;
  try {
    const sqliteKeyMap = {
      sales: 'customer_sales', transactions: 'payment_transactions',
      rep_sales: 'rep_sales', expenses: 'expenses', production: 'mfg_pro_pkr',
      returns: 'stock_returns',
    };
    const sqliteKey = sqliteKeyMap[collectionName] || collectionName;

    let recoveredData = null;
    const localDeletionRecords = await sqliteStore.get('deletion_records', []);
    const tombstoneLocal = Array.isArray(localDeletionRecords)
      ? localDeletionRecords.find(r => r.id === deletedId || r.recordId === deletedId)
      : null;
    if (tombstoneLocal && tombstoneLocal.snapshot) {
      recoveredData = tombstoneLocal.snapshot;
    }

    let cleanRecord = null;
    if (recoveredData) {
      cleanRecord = { ...recoveredData };
      delete cleanRecord.deletedAt;
      delete cleanRecord.tombstoned_at;
      delete cleanRecord.deleted_by;
      delete cleanRecord.deletion_version;
      delete cleanRecord.recoveredAt;
      delete cleanRecord._placeholder;
      delete cleanRecord.isDeleted;
      delete cleanRecord.softDeleted;
      cleanRecord.updatedAt   = Date.now();
      cleanRecord.recoveredAt = Date.now();
      cleanRecord.syncedAt    = new Date().toISOString();
    }

    const newId  = generateUUID('recovered');
    const oldId  = String(deletedId);
    if (cleanRecord) {
      cleanRecord.id = newId;
      delete cleanRecord.originalId;
    }

    await purgeRecoveredId(oldId, collectionName, cleanRecord, newId, { sqliteStore, OfflineQueue, firebaseDB, currentUser });

    if (cleanRecord && sqliteKey) {
      let localArr = await sqliteStore.get(sqliteKey, []);
      if (!Array.isArray(localArr)) localArr = [];
      localArr = localArr.filter(r => r.id !== oldId && r.id !== newId);
      localArr.push(cleanRecord);
      await sqliteStore.set(sqliteKey, localArr);
    }

    return true;
  } catch(e) {
    console.error('[RecycleBin] recoverRecord error:', _safeErr(e));
    return false;
  }
}

function makeTombstone(id, overrides = {}) {
  return {
    id,
    recordId: id,
    collection: 'sales',
    recordType: 'sales',
    deletedAt: Date.now() - 60_000,
    tombstoned_at: Date.now() - 60_000,
    deleted_by: 'user',
    deletion_version: '2.0',
    displayName: 'Test Customer',
    snapshot: {
      id,
      customerName: 'Test Customer',
      totalValue: 5000,
      paymentType: 'CASH',
      date: '2024-06-01',
      createdAt: Date.now() - 120_000,
      originalId: id,
      isDeleted: true,
      softDeleted: true,
      deletedAt: Date.now() - 60_000,
    },
    ...overrides,
  };
}

describe('recoverRecord', () => {
  let sqliteStore, OfflineQueue;

  beforeEach(() => {
    deletedRecordIds = new Set();
    deletionRecords  = [];
    sqliteStore      = makeSqliteStore();
    OfflineQueue     = makeOfflineQueue();
  });

  describe('return values', () => {
    it('returns false when deletedId is falsy', async () => {
      const result = await recoverRecord(null, 'sales', { sqliteStore, OfflineQueue });
      assert.equal(result, false);
    });

    it('returns false when collectionName is falsy', async () => {
      const result = await recoverRecord('some-id', '', { sqliteStore, OfflineQueue });
      assert.equal(result, false);
    });

    it('returns true when record is in deletion_records with snapshot', async () => {
      const id = generateUUID('test');
      await sqliteStore.set('deletion_records', [makeTombstone(id)]);
      deletedRecordIds.add(id);
      const result = await recoverRecord(id, 'sales', { sqliteStore, OfflineQueue });
      assert.equal(result, true);
    });

    it('returns true even when there is no snapshot (id-only tombstone)', async () => {
      const id = generateUUID('test');
      await sqliteStore.set('deletion_records', [{ id, recordId: id, collection: 'sales', snapshot: null }]);
      deletedRecordIds.add(id);
      const result = await recoverRecord(id, 'sales', { sqliteStore, OfflineQueue });
      assert.equal(result, true);
    });
  });

  describe('deletedRecordIds cleanup', () => {
    it('removes the id from deletedRecordIds', async () => {
      const id = generateUUID('test');
      deletedRecordIds.add(id);
      await sqliteStore.set('deletion_records', [makeTombstone(id)]);
      await recoverRecord(id, 'sales', { sqliteStore, OfflineQueue });
      assert.equal(deletedRecordIds.has(id), false);
    });

    it('does not affect other ids in deletedRecordIds', async () => {
      const id1 = generateUUID('test');
      const id2 = generateUUID('test');
      deletedRecordIds.add(id1);
      deletedRecordIds.add(id2);
      await sqliteStore.set('deletion_records', [makeTombstone(id1)]);
      await recoverRecord(id1, 'sales', { sqliteStore, OfflineQueue });
      assert.equal(deletedRecordIds.has(id2), true);
    });
  });

  describe('deletion_records cleanup', () => {
    it('removes tombstone from SQLite deletion_records', async () => {
      const id = generateUUID('test');
      await sqliteStore.set('deletion_records', [makeTombstone(id)]);
      deletedRecordIds.add(id);
      await recoverRecord(id, 'sales', { sqliteStore, OfflineQueue });
      const remaining = await sqliteStore.get('deletion_records', []);
      assert.equal(remaining.length, 0);
    });

    it('only removes the matching tombstone, leaves others', async () => {
      const id1 = generateUUID('test');
      const id2 = generateUUID('test');
      await sqliteStore.set('deletion_records', [makeTombstone(id1), makeTombstone(id2)]);
      deletedRecordIds.add(id1);
      await recoverRecord(id1, 'sales', { sqliteStore, OfflineQueue });
      const remaining = await sqliteStore.get('deletion_records', []);
      assert.equal(remaining.length, 1);
      assert.equal(remaining[0].id, id2);
    });

    it('also matches on recordId field, not just id', async () => {
      const id = generateUUID('test');
      const tombstone = { ...makeTombstone('other'), recordId: id };
      await sqliteStore.set('deletion_records', [tombstone]);
      deletedRecordIds.add(id);
      await recoverRecord(id, 'sales', { sqliteStore, OfflineQueue });
      const remaining = await sqliteStore.get('deletion_records', []);
      assert.equal(remaining.length, 0);
    });
  });

  describe('snapshot field cleaning', () => {
    it('restored record has deletion fields stripped', async () => {
      const id = generateUUID('test');
      await sqliteStore.set('deletion_records', [makeTombstone(id)]);
      deletedRecordIds.add(id);
      await recoverRecord(id, 'sales', { sqliteStore, OfflineQueue });
      const restored = await sqliteStore.get('customer_sales', []);
      assert.equal(restored.length, 1);
      const r = restored[0];
      assert.equal(r.deletedAt,        undefined);
      assert.equal(r.tombstoned_at,     undefined);
      assert.equal(r.deleted_by,        undefined);
      assert.equal(r.deletion_version,  undefined);
      assert.equal(r.isDeleted,         undefined);
      assert.equal(r.softDeleted,       undefined);
      assert.equal(r._placeholder,      undefined);
    });

    it('restored record has originalId removed', async () => {
      const id = generateUUID('test');
      await sqliteStore.set('deletion_records', [makeTombstone(id)]);
      deletedRecordIds.add(id);
      await recoverRecord(id, 'sales', { sqliteStore, OfflineQueue });
      const restored = await sqliteStore.get('customer_sales', []);
      assert.equal(restored[0].originalId, undefined);
    });

    it('restored record gets a fresh UUID prefixed with "recovered"', async () => {
      const id = generateUUID('test');
      await sqliteStore.set('deletion_records', [makeTombstone(id)]);
      deletedRecordIds.add(id);
      await recoverRecord(id, 'sales', { sqliteStore, OfflineQueue });
      const restored = await sqliteStore.get('customer_sales', []);
      assert.ok(restored[0].id.startsWith('recovered-'), `id should start with "recovered-", got: ${restored[0].id}`);
      assert.notEqual(restored[0].id, id);
    });

    it('new id is a valid UUID', async () => {
      const id = generateUUID('test');
      await sqliteStore.set('deletion_records', [makeTombstone(id)]);
      deletedRecordIds.add(id);
      await recoverRecord(id, 'sales', { sqliteStore, OfflineQueue });
      const restored = await sqliteStore.get('customer_sales', []);
      assert.ok(validateUUID(restored[0].id), `expected valid UUID, got: ${restored[0].id}`);
    });

    it('restored record has recoveredAt set', async () => {
      const before = Date.now();
      const id = generateUUID('test');
      await sqliteStore.set('deletion_records', [makeTombstone(id)]);
      deletedRecordIds.add(id);
      await recoverRecord(id, 'sales', { sqliteStore, OfflineQueue });
      const restored = await sqliteStore.get('customer_sales', []);
      assert.ok(restored[0].recoveredAt >= before);
    });

    it('restored record has updatedAt refreshed', async () => {
      const before = Date.now();
      const id = generateUUID('test');
      await sqliteStore.set('deletion_records', [makeTombstone(id)]);
      deletedRecordIds.add(id);
      await recoverRecord(id, 'sales', { sqliteStore, OfflineQueue });
      const restored = await sqliteStore.get('customer_sales', []);
      assert.ok(restored[0].updatedAt >= before);
    });

    it('preserves original business data from snapshot', async () => {
      const id = generateUUID('test');
      await sqliteStore.set('deletion_records', [makeTombstone(id)]);
      deletedRecordIds.add(id);
      await recoverRecord(id, 'sales', { sqliteStore, OfflineQueue });
      const restored = await sqliteStore.get('customer_sales', []);
      assert.equal(restored[0].customerName, 'Test Customer');
      assert.equal(restored[0].totalValue, 5000);
    });
  });

  describe('SQLite write-back', () => {
    it('writes the cleaned record to the correct SQLite key for the collection', async () => {
      const id = generateUUID('test');
      await sqliteStore.set('deletion_records', [makeTombstone(id)]);
      deletedRecordIds.add(id);
      await recoverRecord(id, 'sales', { sqliteStore, OfflineQueue });
      const inSales = await sqliteStore.get('customer_sales', []);
      assert.equal(inSales.length, 1);
    });

    it('appends to existing records in SQLite rather than replacing all', async () => {
      const id = generateUUID('test');
      const existing = { id: generateUUID('existing'), customerName: 'Pre-existing', totalValue: 1000 };
      await sqliteStore.set('customer_sales', [existing]);
      await sqliteStore.set('deletion_records', [makeTombstone(id)]);
      deletedRecordIds.add(id);
      await recoverRecord(id, 'sales', { sqliteStore, OfflineQueue });
      const all = await sqliteStore.get('customer_sales', []);
      assert.equal(all.length, 2);
    });

    it('does not duplicate if old id already in SQLite array', async () => {
      const id = generateUUID('test');
      const stale = { id, customerName: 'Stale', totalValue: 0 };
      await sqliteStore.set('customer_sales', [stale]);
      await sqliteStore.set('deletion_records', [makeTombstone(id)]);
      deletedRecordIds.add(id);
      await recoverRecord(id, 'sales', { sqliteStore, OfflineQueue });
      const all = await sqliteStore.get('customer_sales', []);
      const byOldId = all.filter(r => r.id === id);
      assert.equal(byOldId.length, 0);
    });
  });

  describe('OfflineQueue stale-op pruning', () => {
    it('removes stale delete op for recovered id from queue', async () => {
      const id = generateUUID('test');
      OfflineQueue.queue = [
        { operation: { action: 'delete', docId: id } },
        { operation: { action: 'delete', docId: 'unrelated-id' } },
      ];
      await sqliteStore.set('deletion_records', [makeTombstone(id)]);
      deletedRecordIds.add(id);
      await recoverRecord(id, 'sales', { sqliteStore, OfflineQueue });
      const remaining = OfflineQueue.queue.map(i => i.operation.docId);
      assert.ok(!remaining.includes(id));
      assert.ok(remaining.includes('unrelated-id'));
    });

    it('removes null-data set op for recovered id from queue', async () => {
      const id = generateUUID('test');
      OfflineQueue.queue = [
        { operation: { action: 'set', docId: id, data: null } },
      ];
      await sqliteStore.set('deletion_records', [makeTombstone(id)]);
      deletedRecordIds.add(id);
      await recoverRecord(id, 'sales', { sqliteStore, OfflineQueue });
      assert.equal(OfflineQueue.queue.length, 0);
    });

    it('does not remove valid set op with data for recovered id', async () => {
      const id = generateUUID('test');
      OfflineQueue.queue = [
        { operation: { action: 'set', docId: id, data: { id, name: 'keep' } } },
      ];
      await sqliteStore.set('deletion_records', [makeTombstone(id)]);
      deletedRecordIds.add(id);
      await recoverRecord(id, 'sales', { sqliteStore, OfflineQueue });
      assert.equal(OfflineQueue.queue.length, 1);
    });

    it('prunes dead letter queue of stale delete ops', async () => {
      const id = generateUUID('test');
      OfflineQueue.deadLetterQueue = [
        { operation: { action: 'delete', docId: id } },
      ];
      await sqliteStore.set('deletion_records', [makeTombstone(id)]);
      deletedRecordIds.add(id);
      await recoverRecord(id, 'sales', { sqliteStore, OfflineQueue });
      assert.equal(OfflineQueue.deadLetterQueue.length, 0);
    });

    it('works correctly when OfflineQueue is null', async () => {
      const id = generateUUID('test');
      await sqliteStore.set('deletion_records', [makeTombstone(id)]);
      deletedRecordIds.add(id);
      const result = await recoverRecord(id, 'sales', { sqliteStore, OfflineQueue: null });
      assert.equal(result, true);
    });
  });

  describe('collection key mapping', () => {
    it('maps "transactions" collection to payment_transactions sqlite key', async () => {
      const id = generateUUID('test');
      const tombstone = makeTombstone(id);
      tombstone.collection = 'transactions';
      tombstone.snapshot = { ...tombstone.snapshot, entityName: 'Test Entity', amount: 1000 };
      await sqliteStore.set('deletion_records', [tombstone]);
      deletedRecordIds.add(id);
      await recoverRecord(id, 'transactions', { sqliteStore, OfflineQueue });
      const txns = await sqliteStore.get('payment_transactions', []);
      assert.equal(txns.length, 1);
    });

    it('maps "production" collection to mfg_pro_pkr sqlite key', async () => {
      const id = generateUUID('test');
      const tombstone = makeTombstone(id);
      tombstone.collection = 'production';
      tombstone.snapshot = { ...tombstone.snapshot, store: 'standard', net: 50 };
      await sqliteStore.set('deletion_records', [tombstone]);
      deletedRecordIds.add(id);
      await recoverRecord(id, 'production', { sqliteStore, OfflineQueue });
      const prod = await sqliteStore.get('mfg_pro_pkr', []);
      assert.equal(prod.length, 1);
    });
  });
});
