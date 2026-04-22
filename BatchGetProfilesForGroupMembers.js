import { util } from '@aws-appsync/utils';

const TABLE_NAME = 'SplitRickDB';

/**
 * PIPELINE STEP 2: BatchGet PROFILE records for each membership from step 1
 *
 * Step 1 should return an array of membership items of the form:
 *   { userId, groupId, role, joinedAt, ... }
 */
export function request(ctx) {
  // Memberships from previous function
  const prev = ctx && ctx.prev && Array.isArray(ctx.prev.result)
    ? ctx.prev.result
    : [];

  const memberships = prev;

  // Collect userIds
  const userIds = memberships
    .map(m => (m && m.userId) ? m.userId : null)
    .filter(id => id !== null && id !== undefined && id !== '');

  if (userIds.length === 0) {
    const emptyTables = {};
    emptyTables[TABLE_NAME] = { keys: [], consistentRead: false };
    return {
      operation: 'BatchGetItem',
      tables: emptyTables
    };
  }

  // Build keys for PROFILE items
  const keys = userIds.map(userId => ({
    PK: { S: 'USER#' + userId },
    SK: { S: 'PROFILE' }
  }));

  const tables = {};
  tables[TABLE_NAME] = {
    keys: keys,
    consistentRead: false
  };

  return {
    operation: 'BatchGetItem',
    tables: tables
  };
}

/**
 * RESPONSE: join membership rows with PROFILE records into GroupMember DTOs
 *
 * Output shape per item:
 *   {
 *     userId,
 *     groupId,
 *     role,
 *     joinedAt,
 *     displayName,
 *     avatarKey
 *   }
 */
export function response(ctx) {
  if (ctx && ctx.error) {
    const msg = ctx.error.message ? ctx.error.message : '' + ctx.error;
    util.error(msg);
  }

  // Membership rows from step 1
  const prev = ctx && ctx.prev && Array.isArray(ctx.prev.result)
    ? ctx.prev.result
    : [];
  const memberships = prev;

  // Map userId -> membership
  const membershipByUserId = {};
  memberships.forEach(m => {
    if (!m) {
      return;
    }
    const uid = m.userId;
    if (uid) {
      membershipByUserId[uid] = m;
    }
  });

  // Raw BatchGetItem data
  const data = ctx && ctx.result && ctx.result.data ? ctx.result.data : {};
  const rawProfiles = data[TABLE_NAME] || [];

  // Minimal unmarshaller for DynamoDB AttributeValues
  function get(av) {
    if (av && typeof av === 'object') {
      if (av.S !== undefined) return av.S;
      if (av.N !== undefined) return av.N;
      if (av.BOOL !== undefined) return av.BOOL;
      if (av.NULL !== undefined) return null;
    }
    return av;
  }

  function extractUserId(profileItem) {
    // 1) direct userId attribute
    const direct = get(profileItem.userId);
    if (direct) return direct;

    // 2) derive from PK = USER#{id}
    const pk = get(profileItem.PK);
    if (pk && pk.indexOf('USER#') === 0) {
      return pk.substring('USER#'.length);
    }

    return null;
  }

  const seenUserIds = {};
  const members = [];

  // Build DTOs for profiles that exist
  rawProfiles.forEach(item => {
    if (!item) {
      return;
    }

    const userId = extractUserId(item);
    if (!userId) {
      return;
    }

    seenUserIds[userId] = true;

    const membership = membershipByUserId[userId];

    const dto = {
      userId: userId,
      groupId: membership && membership.groupId ? membership.groupId : null,
      role: membership && membership.role ? membership.role : null,
      joinedAt: membership && membership.joinedAt ? membership.joinedAt : null,
      displayName: get(item.displayName) || null,
      avatarKey: get(item.profileImageKey) || null
    };

    members.push(dto);
  });

  // Include memberships that had no PROFILE row (fallback)
  memberships.forEach(mem => {
    if (!mem) {
      return;
    }
    const memUid = mem.userId;
    if (!memUid || seenUserIds[memUid]) {
      return;
    }

    members.push({
      userId: memUid,
      groupId: mem.groupId || null,
      role: mem.role || null,
      joinedAt: mem.joinedAt || null,
      displayName: null,
      avatarKey: null
    });
  });

  return members;
}