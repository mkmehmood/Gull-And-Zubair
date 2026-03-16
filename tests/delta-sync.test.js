import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';

let deletedRecordIds = new Set();

function _toMs(v) {
  if (!v) return 0;
  if (typeof v === 'number') return v;
  if (typeof v.toMillis === 'function') return v.toMillis();
  if (typeof v === 'object' && v.seconds) return v.seconds * 1000 + Math.round((v.nanoseconds || 0) / 1e6);
  return new Date(v).getTime() || 0;
}

function mergeDatasets(localArray, cloudArray) {
  if (!Array.isArray(localArray)) localArray = [];
  if (!Array.isArray(cloudArray)) cloudArray = [];
  const mergedMap = new Map();
  cloudArray.forEach(item => {
    if (item && item.id) {
      if (deletedRecordIds.has(item.id)) return;
      mergedMap.set(item.id, item);
    }
  });
  localArray.forEach(localItem => {
    if (!localItem || !localItem.id) return;
    if (deletedRecordIds.has(localItem.id)) return;
    const cloudItem = mergedMap.get(localItem.id);
    if (!cloudItem) { mergedMap.set(localItem.id, localItem); return; }
    const isFinancialRecord = (localItem.totalSold !== undefined || localItem.revenue !== undefined);
    if (isFinancialRecord) {
      const localHasData  = (localItem.totalSold > 0 || localItem.revenue > 0);
      const cloudIsCorrupt = (cloudItem.totalSold === undefined || cloudItem.totalSold === null || cloudItem.revenue === null);
      if (localHasData && cloudIsCorrupt) { mergedMap.set(localItem.id, localItem); return; }
    }
    if (localItem.isReturn === true && !cloudItem.isReturn) { mergedMap.set(localItem.id, localItem); return; }
    if ((localItem.formulaUnits > 0 && !cloudItem.formulaUnits) || (localItem.formulaCost > 0 && !cloudItem.formulaCost)) {
      mergedMap.set(localItem.id, localItem); return;
    }
    if (localItem.supplierId && !cloudItem.supplierId) { mergedMap.set(localItem.id, localItem); return; }
    if (localItem.paymentStatus === 'paid' && cloudItem.paymentStatus !== 'paid') {
      mergedMap.set(localItem.id, localItem); return;
    }
    const localTime = _toMs(localItem.updatedAt || localItem.timestamp) || new Date(localItem.date).getTime() || 0;
    const cloudTime = _toMs(cloudItem.updatedAt || cloudItem.timestamp) || new Date(cloudItem.date).getTime() || 0;
    if (localTime >= cloudTime) mergedMap.set(localItem.id, localItem);
  });
  return Array.from(mergedMap.values());
}

function sanitizeForFirestore(obj, depth = 0, seen = new WeakSet()) {
  if (depth > 20) return null;
  if (obj === null || obj === undefined) return null;
  if (obj instanceof Date) return obj.toISOString();
  if (typeof obj === 'object') {
    if (seen.has(obj)) return '[Circular]';
    seen.add(obj);
  }
  if (typeof obj !== 'object') {
    if (typeof obj === 'number') { if (isNaN(obj) || !isFinite(obj)) return 0; return obj; }
    if (typeof obj === 'string') return obj;
    if (typeof obj === 'boolean') return obj;
    try { return String(obj); } catch (e) { return null; }
  }
  if (Array.isArray(obj)) {
    const out = [];
    for (const item of obj) {
      if (typeof item === 'function') continue;
      const s = sanitizeForFirestore(item, depth + 1, seen);
      if (s !== null && s !== undefined) out.push(s);
    }
    return out;
  }
  const sanitized = {};
  try {
    for (const key in obj) {
      if (!obj.hasOwnProperty(key)) continue;
      const value = obj[key];
      if (!key || typeof key !== 'string') continue;
      if (typeof value === 'function') continue;
      let cleanKey = key.replace(/[.\$#\[\]\/\\]/g, '_');
      if (!cleanKey) continue;
      if (cleanKey === 'id') {
        sanitized[cleanKey] = (value === null || value === undefined) ? '' : String(value);
        continue;
      }
      if (['amount', 'quantity', 'price', 'cost'].includes(cleanKey)) {
        const num = parseFloat(value);
        sanitized[cleanKey] = (isNaN(num) || !isFinite(num)) ? 0 : num;
        continue;
      }
      if (['timestamp', 'createdAt', 'updatedAt'].includes(cleanKey)) {
        if (value instanceof Date) sanitized[cleanKey] = value.toISOString();
        else if (typeof value === 'string' || typeof value === 'number') sanitized[cleanKey] = value;
        else sanitized[cleanKey] = new Date().toISOString();
        continue;
      }
      const sv = sanitizeForFirestore(value, depth + 1, seen);
      if (sv !== null && sv !== undefined) {
        if (typeof sv === 'object' && !Array.isArray(sv)) {
          const isFactorySettings = ['default_formulas','additional_costs','cost_adjustment_factor',
            'sale_prices','unit_tracking','standard','asaan'].includes(cleanKey);
          if (Object.keys(sv).length > 0 || isFactorySettings) sanitized[cleanKey] = sv;
        } else {
          sanitized[cleanKey] = sv;
        }
      } else if (cleanKey === 'gps') {
        sanitized[cleanKey] = null;
      }
    }
  } catch(e) { return {}; }
  return sanitized;
}

const T_OLD = 1_700_000_000_000;
const T_NEW = 1_700_000_001_000;

function rec(overrides) {
  return { id: 'r1', updatedAt: T_OLD, ...overrides };
}

describe('mergeDatasets', () => {

  beforeEach(() => { deletedRecordIds = new Set(); });

  describe('inputs', () => {
    it('returns empty array when both inputs are empty', () => {
      assert.deepEqual(mergeDatasets([], []), []);
    });

    it('handles null/undefined inputs gracefully', () => {
      assert.deepEqual(mergeDatasets(null, null), []);
      assert.deepEqual(mergeDatasets(undefined, undefined), []);
    });

    it('skips items with no id', () => {
      const result = mergeDatasets([{ updatedAt: T_NEW }], [{ updatedAt: T_OLD }]);
      assert.equal(result.length, 0);
    });

    it('cloud-only record is included', () => {
      const result = mergeDatasets([], [{ id: 'c1', name: 'cloud' }]);
      assert.equal(result.length, 1);
      assert.equal(result[0].id, 'c1');
    });

    it('local-only record is included', () => {
      const result = mergeDatasets([{ id: 'l1', name: 'local' }], []);
      assert.equal(result.length, 1);
      assert.equal(result[0].id, 'l1');
    });
  });

  describe('deletion filter', () => {
    it('excludes cloud record whose id is in deletedRecordIds', () => {
      deletedRecordIds.add('c1');
      const result = mergeDatasets([], [{ id: 'c1', name: 'cloud' }]);
      assert.equal(result.length, 0);
    });

    it('excludes local record whose id is in deletedRecordIds', () => {
      deletedRecordIds.add('l1');
      const result = mergeDatasets([{ id: 'l1', name: 'local' }], []);
      assert.equal(result.length, 0);
    });

    it('only filters the deleted id, not other records', () => {
      deletedRecordIds.add('r1');
      const result = mergeDatasets(
        [rec({ id: 'r1' }), rec({ id: 'r2' })],
        []
      );
      assert.equal(result.length, 1);
      assert.equal(result[0].id, 'r2');
    });
  });

  describe('timestamp conflict resolution', () => {
    it('local wins when local is newer', () => {
      const local = rec({ name: 'local-newer', updatedAt: T_NEW });
      const cloud = rec({ name: 'cloud-older', updatedAt: T_OLD });
      const result = mergeDatasets([local], [cloud]);
      assert.equal(result[0].name, 'local-newer');
    });

    it('cloud wins when cloud is strictly newer', () => {
      const local = rec({ name: 'local-older', updatedAt: T_OLD });
      const cloud = rec({ name: 'cloud-newer', updatedAt: T_NEW });
      const result = mergeDatasets([local], [cloud]);
      assert.equal(result[0].name, 'cloud-newer');
    });

    it('local wins on exact timestamp tie', () => {
      const local = rec({ name: 'local-tie', updatedAt: T_OLD });
      const cloud = rec({ name: 'cloud-tie', updatedAt: T_OLD });
      const result = mergeDatasets([local], [cloud]);
      assert.equal(result[0].name, 'local-tie');
    });

    it('falls back to timestamp field when updatedAt is absent', () => {
      const local = { id: 'r1', name: 'local', timestamp: T_NEW };
      const cloud = { id: 'r1', name: 'cloud', timestamp: T_OLD };
      const result = mergeDatasets([local], [cloud]);
      assert.equal(result[0].name, 'local');
    });

    it('uses date string when updatedAt and timestamp are absent', () => {
      const local = { id: 'r1', name: 'local', date: '2024-03-15' };
      const cloud = { id: 'r1', name: 'cloud', date: '2024-01-01' };
      const result = mergeDatasets([local], [cloud]);
      assert.equal(result[0].name, 'local');
    });

    it('handles Firestore-style {seconds, nanoseconds} timestamp', () => {
      const local = rec({ name: 'local', updatedAt: { seconds: 1700000001, nanoseconds: 0 } });
      const cloud = rec({ name: 'cloud', updatedAt: { seconds: 1700000000, nanoseconds: 0 } });
      const result = mergeDatasets([local], [cloud]);
      assert.equal(result[0].name, 'local');
    });

    it('handles .toMillis() Firestore Timestamp object', () => {
      const local = rec({ name: 'local', updatedAt: { toMillis: () => T_NEW } });
      const cloud = rec({ name: 'cloud', updatedAt: { toMillis: () => T_OLD } });
      const result = mergeDatasets([local], [cloud]);
      assert.equal(result[0].name, 'local');
    });
  });

  describe('semantic conflict guards', () => {
    it('local wins when local has financial data and cloud is corrupt (totalSold null)', () => {
      const local = rec({ name: 'local', totalSold: 100, revenue: 5000, updatedAt: T_OLD });
      const cloud = rec({ name: 'cloud', totalSold: null, revenue: null, updatedAt: T_NEW });
      const result = mergeDatasets([local], [cloud]);
      assert.equal(result[0].name, 'local');
    });

    it('cloud wins when local financial data is zero and cloud is newer', () => {
      const local = rec({ name: 'local', totalSold: 0, revenue: 0, updatedAt: T_OLD });
      const cloud = rec({ name: 'cloud', totalSold: 50, revenue: 2500, updatedAt: T_NEW });
      const result = mergeDatasets([local], [cloud]);
      assert.equal(result[0].name, 'cloud');
    });

    it('local wins when local isReturn=true and cloud has no isReturn', () => {
      const local = rec({ name: 'local', isReturn: true, updatedAt: T_OLD });
      const cloud = rec({ name: 'cloud', updatedAt: T_NEW });
      const result = mergeDatasets([local], [cloud]);
      assert.equal(result[0].name, 'local');
    });

    it('does not apply isReturn guard when both have isReturn', () => {
      const local = rec({ name: 'local', isReturn: true, updatedAt: T_OLD });
      const cloud = rec({ name: 'cloud', isReturn: true, updatedAt: T_NEW });
      const result = mergeDatasets([local], [cloud]);
      assert.equal(result[0].name, 'cloud');
    });

    it('local wins when local has formulaUnits and cloud does not', () => {
      const local = rec({ name: 'local', formulaUnits: 10, updatedAt: T_OLD });
      const cloud = rec({ name: 'cloud', updatedAt: T_NEW });
      const result = mergeDatasets([local], [cloud]);
      assert.equal(result[0].name, 'local');
    });

    it('local wins when local has formulaCost and cloud does not', () => {
      const local = rec({ name: 'local', formulaCost: 500, updatedAt: T_OLD });
      const cloud = rec({ name: 'cloud', updatedAt: T_NEW });
      const result = mergeDatasets([local], [cloud]);
      assert.equal(result[0].name, 'local');
    });

    it('local wins when local has supplierId and cloud does not', () => {
      const local = rec({ name: 'local', supplierId: 'sup-1', updatedAt: T_OLD });
      const cloud = rec({ name: 'cloud', updatedAt: T_NEW });
      const result = mergeDatasets([local], [cloud]);
      assert.equal(result[0].name, 'local');
    });

    it('local wins when local paymentStatus is paid and cloud is not', () => {
      const local = rec({ name: 'local', paymentStatus: 'paid', updatedAt: T_OLD });
      const cloud = rec({ name: 'cloud', paymentStatus: 'pending', updatedAt: T_NEW });
      const result = mergeDatasets([local], [cloud]);
      assert.equal(result[0].name, 'local');
    });

    it('does not apply paymentStatus guard when cloud is also paid', () => {
      const local = rec({ name: 'local', paymentStatus: 'paid', updatedAt: T_OLD });
      const cloud = rec({ name: 'cloud', paymentStatus: 'paid', updatedAt: T_NEW });
      const result = mergeDatasets([local], [cloud]);
      assert.equal(result[0].name, 'cloud');
    });
  });

  describe('multiple records', () => {
    it('merges non-conflicting records from both sides', () => {
      const result = mergeDatasets(
        [{ id: 'l1', name: 'local-1' }],
        [{ id: 'c1', name: 'cloud-1' }]
      );
      assert.equal(result.length, 2);
    });

    it('deduplicates — no record appears twice', () => {
      const shared = rec({ name: 'shared', updatedAt: T_NEW });
      const result = mergeDatasets([shared], [shared]);
      assert.equal(result.length, 1);
    });

    it('each conflicting id resolved independently', () => {
      const result = mergeDatasets(
        [
          { id: 'a', name: 'local-a', updatedAt: T_NEW },
          { id: 'b', name: 'local-b', updatedAt: T_OLD },
        ],
        [
          { id: 'a', name: 'cloud-a', updatedAt: T_OLD },
          { id: 'b', name: 'cloud-b', updatedAt: T_NEW },
        ]
      );
      const byId = Object.fromEntries(result.map(r => [r.id, r]));
      assert.equal(byId['a'].name, 'local-a');
      assert.equal(byId['b'].name, 'cloud-b');
    });
  });
});

describe('sanitizeForFirestore', () => {

  it('returns null for null input', () => {
    assert.equal(sanitizeForFirestore(null), null);
  });

  it('returns null for undefined input', () => {
    assert.equal(sanitizeForFirestore(undefined), null);
  });

  it('converts Date to ISO string', () => {
    const d = new Date('2024-01-15T10:00:00.000Z');
    assert.equal(sanitizeForFirestore(d), d.toISOString());
  });

  it('replaces NaN with 0', () => {
    const result = sanitizeForFirestore({ amount: NaN });
    assert.equal(result.amount, 0);
  });

  it('replaces Infinity with 0', () => {
    const result = sanitizeForFirestore({ cost: Infinity });
    assert.equal(result.cost, 0);
  });

  it('coerces numeric string for amount field', () => {
    const result = sanitizeForFirestore({ amount: '1500.5' });
    assert.equal(result.amount, 1500.5);
  });

  it('coerces null amount to 0', () => {
    const result = sanitizeForFirestore({ amount: null });
    assert.equal(result.amount, 0);
  });

  it('casts id to string', () => {
    const result = sanitizeForFirestore({ id: 12345 });
    assert.equal(result.id, '12345');
    assert.equal(typeof result.id, 'string');
  });

  it('sets id to empty string when null', () => {
    const result = sanitizeForFirestore({ id: null });
    assert.equal(result.id, '');
  });

  it('sanitizes illegal Firestore key characters', () => {
    const result = sanitizeForFirestore({ 'a.b': 'val' });
    assert.ok('a_b' in result);
    assert.ok(!('a.b' in result));
  });

  it('sanitizes $ in key', () => {
    const result = sanitizeForFirestore({ '$field': 'val' });
    assert.ok('_field' in result);
  });

  it('removes function-valued fields', () => {
    const result = sanitizeForFirestore({ fn: () => {}, name: 'ok' });
    assert.ok(!('fn' in result));
    assert.equal(result.name, 'ok');
  });

  it('handles circular reference without throwing', () => {
    const obj = { name: 'test' };
    obj.self = obj;
    assert.doesNotThrow(() => sanitizeForFirestore(obj));
    const result = sanitizeForFirestore(obj);
    assert.equal(result.self, '[Circular]');
  });

  it('recursively sanitizes nested objects', () => {
    const result = sanitizeForFirestore({ nested: { amount: NaN, id: 42 } });
    assert.equal(result.nested.amount, 0);
    assert.equal(result.nested.id, '42');
  });

  it('sanitizes array items', () => {
    const result = sanitizeForFirestore({ items: [{ amount: NaN }, { amount: 100 }] });
    assert.equal(result.items[0].amount, 0);
    assert.equal(result.items[1].amount, 100);
  });

  it('drops function items from arrays', () => {
    const result = sanitizeForFirestore([() => {}, 'keep']);
    assert.equal(result.length, 1);
    assert.equal(result[0], 'keep');
  });

  it('sets gps to null when value sanitizes to null', () => {
    const result = sanitizeForFirestore({ gps: null });
    assert.ok('gps' in result);
    assert.equal(result.gps, null);
  });

  it('returns null beyond depth limit of 20', () => {
    let deep = { value: 'leaf' };
    for (let i = 0; i < 22; i++) deep = { child: deep };
    assert.doesNotThrow(() => sanitizeForFirestore(deep));
  });

  it('preserves boolean values', () => {
    const result = sanitizeForFirestore({ active: true, deleted: false });
    assert.equal(result.active, true);
    assert.equal(result.deleted, false);
  });

  it('passes through valid timestamp string unchanged', () => {
    const iso = '2024-06-01T12:00:00.000Z';
    const result = sanitizeForFirestore({ updatedAt: iso });
    assert.equal(result.updatedAt, iso);
  });

  it('passes through numeric timestamp unchanged', () => {
    const result = sanitizeForFirestore({ createdAt: 1700000000000 });
    assert.equal(result.createdAt, 1700000000000);
  });

  it('drops empty nested objects that are not factory settings keys', () => {
    const result = sanitizeForFirestore({ meta: {} });
    assert.ok(!('meta' in result));
  });

  it('keeps empty object for factory settings key "standard"', () => {
    const result = sanitizeForFirestore({ standard: {} });
    assert.ok('standard' in result);
  });
});
