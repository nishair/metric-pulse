module.exports = {
  env: {
    es2022: true,
    node: true,
  },
  extends: [
    'eslint:recommended',
  ],
  parserOptions: {
    ecmaVersion: 'latest',
    sourceType: 'module',
  },
  rules: {
    // Error prevention
    'no-console': 'off', // Allow console.log in Node.js applications
    'no-unused-vars': ['error', {
      argsIgnorePattern: '^_',
      varsIgnorePattern: '^_',
    }],
    'no-undef': 'error',
    'no-unreachable': 'error',
    'no-duplicate-imports': 'error',

    // Code quality
    'prefer-const': 'error',
    'no-var': 'error',
    'object-shorthand': 'error',
    'prefer-template': 'error',
    'prefer-arrow-callback': 'error',

    // Async/await
    'no-async-promise-executor': 'error',
    'require-await': 'warn',
    'no-return-await': 'error',

    // Import/Export
    'import/no-unresolved': 'off', // Disable as we're using Node.js modules

    // Style (handled by Prettier)
    'indent': 'off',
    'quotes': 'off',
    'semi': 'off',
    'comma-dangle': 'off',
    'max-len': 'off',

    // Node.js specific
    'no-process-exit': 'warn',
    'no-path-concat': 'error',
  },
  overrides: [
    {
      files: ['test/**/*.js', '**/*.test.js', '**/*.spec.js'],
      env: {
        node: true,
      },
      rules: {
        // Allow more flexible patterns in tests
        'no-unused-expressions': 'off',
        'max-lines-per-function': 'off',
        'max-nested-callbacks': 'off',
      },
    },
    {
      files: ['src/db/migrate.js'],
      rules: {
        // Allow process.exit in migration scripts
        'no-process-exit': 'off',
      },
    },
  ],
};