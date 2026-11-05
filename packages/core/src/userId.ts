/** Allowed characters for a userId (SPEC §6.2). */
export const USER_ID_PATTERN = /^[\w.@+-]+$/;

export const USER_ID_MAX_LENGTH = 254;

export type UserIdResult = { ok: true; value: string } | { ok: false; message: string };

/**
 * Trims and lowercases a userId, then validates it. The same normalisation is
 * applied by the API and by anything that reads a userId back, so a user can
 * always find their reservation regardless of casing or surrounding spaces.
 */
export function normalizeUserId(raw: string): UserIdResult {
  const value = raw.trim().toLowerCase();

  if (value.length === 0) {
    return { ok: false, message: 'userId is required' };
  }
  if (value.length > USER_ID_MAX_LENGTH) {
    return { ok: false, message: `userId must be at most ${USER_ID_MAX_LENGTH} characters` };
  }
  if (!USER_ID_PATTERN.test(value)) {
    return {
      ok: false,
      message: 'userId may only contain letters, numbers, and . @ + - _',
    };
  }

  return { ok: true, value };
}
