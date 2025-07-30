SELECT intspan '[1,2]';
SELECT intspan '(1,2]';
SELECT datespan '[2000-01-01,2000-01-02]';
SELECT datespan '(2000-01-01,2000-01-02]';

SELECT astext( datespan '[2000-01-01,2000-01-02]');