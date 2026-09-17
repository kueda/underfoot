/**
 * Converts an age value into a human-readable string with appropriate units.
 *
 * @param {string | number | undefined} age - The age value to be humanized. It can be a string, number, or undefined.
 * @returns {string} A human-readable string representing the age with appropriate units.
 *
 * - If the age is 0, it returns "0".
 * - If the age is undefined or cannot be parsed, it returns "?".
 * - If the age is 1 billion or more, it returns the value in Ga (billions of years).
 * - If the age is 1 million or more, it returns the value in Ma (millions of years).
 * - If the age is 100 thousand or more, it returns the value in ka (thousands of years).
 * - Otherwise, it returns the age in years.
 */
export function humanizeAge(age?: string | number) {
  const unknown = '?';
  if (age === 0) return '0';
  if (!age) return unknown;
  let ageNum: number;
  if (typeof (age) === 'string') {
    ageNum = parseFloat(age);
    if (ageNum === 0) return unknown;
  }
  else {
    ageNum = age;
  }
  if (ageNum >= 1_000_000_000) {
    return `${(ageNum / 1_000_000_000.0).toLocaleString(undefined, { maximumFractionDigits: 1 })} Ga`;
  }
  if (ageNum >= 1_000_000) {
    return `${(ageNum / 1_000_000.0).toLocaleString(undefined, { maximumFractionDigits: 1 })} Ma`;
  }
  if (ageNum >= 100_000) {
    return `${(ageNum / 1000.0).toLocaleString(undefined, { maximumFractionDigits: 1 })} ka`;
  }
  return `${ageNum.toLocaleString()} years`;
}
