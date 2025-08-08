SELECT intspan '[1,2]';
SELECT intspan '(1,2]';
SELECT datespan '[2000-01-01,2000-01-02]';
SELECT datespan '(2000-01-01,2000-01-02]';

SELECT tstzspan('[2001-01-01, 2001-01-02]');
--***[2001-01-01 00:00:00+00, 2001-01-03 00:00:00+00) 

SELECT astext( datespan '[2000-01-01,2000-01-02]');