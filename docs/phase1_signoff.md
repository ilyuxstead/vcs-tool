# results of audit_phase1.py

```
========================================================================
  Phase 1 Exit-Criteria Audit — Command Completeness Checklist
========================================================================
  Command             Status                Test? 
  ------------------  --------------------  ------
  repo.init           ✅ FUNCTIONAL             yes
  repo.clone          ✅ FUNCTIONAL             yes
  repo.status         ✅ FUNCTIONAL             yes
  repo.config         ✅ FUNCTIONAL             yes
  commit.stage        ✅ FUNCTIONAL             yes
  commit.unstage      ✅ FUNCTIONAL             yes
  commit.snapshot     ✅ FUNCTIONAL             yes
  commit.show         ✅ FUNCTIONAL             yes
  history.log         ✅ FUNCTIONAL             yes
  history.diff        ✅ FUNCTIONAL             yes
  history.annotate    ✅ FUNCTIONAL             yes
  branch.create       ✅ FUNCTIONAL             yes
  branch.list         ✅ FUNCTIONAL             yes
  branch.switch       ✅ FUNCTIONAL             yes
  branch.merge        ✅ FUNCTIONAL             yes
  branch.delete       ✅ FUNCTIONAL             yes
  remote.add          ✅ FUNCTIONAL             yes
  remote.list         ✅ FUNCTIONAL             yes
  remote.push         ✅ FUNCTIONAL             yes
  remote.fetch        ✅ FUNCTIONAL             yes
  remote.pull         ✅ FUNCTIONAL             yes
  tag.create          ✅ FUNCTIONAL             yes
  tag.list            ✅ FUNCTIONAL             yes

  Summary
  ────────────────────────────────────────
  ✅ FUNCTIONAL            23
  ────────────────────────────────────────
  Total: 23/23 fully functional  (100%)

  🎉 All Phase 1 commands are functional! Ready for Phase 2.
  ```

  # results of `pytest`

  - ran `pytest --cov=src/vcs --cov-report=term-missing --cov-branch`

  ```
============================================================================================= tests coverage =============================================================================================
____________________________________________________________________________ coverage: platform darwin, python 3.13.7-final-0 ____________________________________________________________________________

Name                          Stmts   Miss Branch BrPart  Cover   Missing
-------------------------------------------------------------------------
src/vcs/__init__.py               1      0      0      0   100%
src/vcs/__main__.py             281     28    110      8    91%   28-29, 46-47, 112, 132, 320-324, 358-362, 365-369, 372-381, 432, 436
src/vcs/branch/__init__.py        3      0      0      0   100%
src/vcs/branch/merge.py          95     17     28      7    79%   40, 44, 64, 69-70, 77, 84-86, 125, 128, 163-164, 166-170
src/vcs/branch/ops.py           119      2     16      2    97%   105->107, 169-170
src/vcs/cli/__init__.py           3      0      0      0   100%
src/vcs/cli/output.py            70      0     40      0   100%
src/vcs/cli/parser.py           119      0      2      0   100%
src/vcs/commit/__init__.py        4      0      0      0   100%
src/vcs/commit/show.py           35      1     10      2    93%   65, 69->59
src/vcs/commit/snapshot.py       64      2     10      1    96%   117-118, 178->186
src/vcs/commit/stage.py          51      2     12      1    95%   23, 95
src/vcs/history/__init__.py       4      0      0      0   100%
src/vcs/history/annotate.py      37      2     16      3    91%   62, 86->95, 89
src/vcs/history/diff.py          58      2     22      2    95%   27, 36->35, 128
src/vcs/history/log.py           44      3     16      2    92%   75, 80-81, 90->89
src/vcs/remote/__init__.py        3      0      0      0   100%
src/vcs/remote/ops.py           208     18     64      8    90%   109, 116-117, 122->147, 126-127, 129->147, 148->147, 192-193, 229->228, 237->236, 313-314, 333-334, 371, 382-383, 396-397, 414-415, 424->423
src/vcs/remote/protocol.py       74      2      8      0    98%   99-100
src/vcs/repo/__init__.py          5      0      0      0   100%
src/vcs/repo/clone.py           129     10     34      3    92%   149-150, 165-166, 197-198, 251, 277->276, 302-303, 306
src/vcs/repo/config.py           78     11     26      2    88%   57-60, 93, 98-99, 130-131, 138-139
src/vcs/repo/init.py             62      1     12      1    97%   122
src/vcs/repo/status.py          109      2     40      2    97%   106, 170
src/vcs/store/__init__.py         5      0      0      0   100%
src/vcs/store/db.py             108      2     24      1    98%   230-231
src/vcs/store/exceptions.py      42      0      2      0   100%
src/vcs/store/models.py          49      0      0      0   100%
src/vcs/store/objects.py         59      3     16      2    93%   118, 120->119, 140-141
-------------------------------------------------------------------------
TOTAL                          1919    108    508     47    94%
Required test coverage of 90.0% reached. Total coverage: 93.53%
========================================================================================== 442 passed in 31.77s ==========================================================================================
  ```