"""
DSSAT input file headers for file_generator.py
"""

def get_header(filetype):
    """
    Returns the relevant DSSAT header.

    Parameters
    ----------
    filetype : str
        'weather' 'experiment'

    Returns
    -------
    str
        DSSAT header with f-string formatting variables.
    """

    return headers[filetype]

headers = {
    'weather': (
    "*WEATHER DATA : {location}\n\n"
    "@ INSI      LAT     LONG  ELEV   TAV   AMP REFHT WNDHT\n"
    "  {INSI}   {LAT:.3f}  {LONG:.3f}    {ELEV:.0f}  {TAV:.1f}  {AMP:.0f}  "
    "{REFHT:.2f}  {WNDHT:.2f}\n"
    ), 
    'batch': (
    "$BATCH({CROP})\n"
    "@FILEX                                                  "
    "                                      "
    "TRTNO     RP     SQ     OP     CO\n"
    ),
    'soil': (
    "*{ROI_code}    XXX   XXXXXXXX   {depth}    ISRIC soilgrids + HC27\n"
    "@SITE        COUNTRY          LAT     LONG SCS Family\n"
    "-99              XX       {LAT:.3f} {LONG:.3F}     {family}"
           )
}
