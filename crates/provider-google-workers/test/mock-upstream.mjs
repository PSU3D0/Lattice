const calls = [];

function tokenResponse(form) {
  const code = form.get("code");
  const refreshToken = form.get("refresh_token");
  const common = {
    access_token: form.has("code") ? `access-token-${code}` : "access-token-private-value",
    expires_in: 3600,
    scope: "https://www.googleapis.com/auth/spreadsheets openid https://www.googleapis.com/auth/gmail.send",
    token_type: "Bearer",
  };
  if (form.get("grant_type") === "authorization_code") {
    const response = code === "missing-refresh-token" ? common : { ...common, refresh_token: "refresh-token-private-value" };
    return code === "with-id-token" ? { ...response, id_token: "header.sensitive-subject.signature" } : response;
  }
  if (refreshToken === "refresh-missing-access-token") {
    const { access_token: _accessToken, ...response } = common;
    return response;
  }
  if (refreshToken === "refresh-missing-expires-in") {
    const { expires_in: _expiresIn, ...response } = common;
    return response;
  }
  if (refreshToken === "refresh-without-scope") {
    const { scope: _scope, ...response } = common;
    return response;
  }
  if (refreshToken === "refresh-wrong-scope") {
    return { ...common, scope: "https://www.googleapis.com/auth/gmail.send https://www.googleapis.com/auth/spreadsheets" };
  }
  if (refreshToken === "refresh-lowercase-bearer") return { ...common, token_type: "bearer" };
  if (refreshToken === "refresh-with-rotation") return { ...common, refresh_token: "rotated-refresh-token-private-value" };
  return common;
}

function tokenInfoResponse(accessToken) {
  const common = {
    aud: "google-client-id-private",
    azp: "google-client-id-private",
    scope: "https://www.googleapis.com/auth/spreadsheets openid https://www.googleapis.com/auth/gmail.send",
    sub: "google-subject-123456",
    exp: "1784023200",
    expires_in: "3599",
    access_type: "offline",
  };
  if (accessToken === "access-token-missing-subject") {
    const { sub: _sub, ...response } = common;
    return response;
  }
  if (accessToken === "access-token-wrong-audience") return { ...common, aud: "different-google-client-id" };
  if (accessToken === "access-token-unknown-claim") return { ...common, unexpected_claim: "not-documented" };
  if (accessToken === "access-token-wrong-scope") {
    return { ...common, scope: "https://www.googleapis.com/auth/gmail.send https://www.googleapis.com/auth/spreadsheets" };
  }
  if (accessToken === "access-token-legacy-subject") {
    const { sub: _sub, ...response } = common;
    return { ...response, user_id: "legacy-google-subject-654321" };
  }
  return common;
}

export default {
  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/__calls") return Response.json(calls);
    const body = request.method === "POST" ? await request.clone().text() : "";
    calls.push({ url: request.url, method: request.method, headers: Object.fromEntries(request.headers), body });
    if (request.url === "https://oauth2.googleapis.com/token") {
      return Response.json(tokenResponse(new URLSearchParams(body)));
    }
    if (new URL(request.url).pathname === "/tokeninfo") {
      return Response.json(tokenInfoResponse(url.searchParams.get("access_token")));
    }
    if (request.url === "https://oauth2.googleapis.com/revoke") {
      return new Response(null, { status: 200 });
    }
    if (url.origin === "https://gmail.googleapis.com") {
      return Response.json({ id: "message-1", threadId: "thread-1", labelIds: ["SENT"] }, { headers: { "x-request-id": "gmail-request-1" } });
    }
    if (url.origin === "https://sheets.googleapis.com" && url.pathname === "/v4/spreadsheets") {
      const title = JSON.parse(body).properties.title;
      if (title === "Unknown response") {
        return Response.json({
          spreadsheetId: "sheet_unknown", spreadsheetUrl: "https://docs.google.com/spreadsheets/d/sheet_unknown/edit",
          properties: { title }, unexpected: "must fail closed",
        });
      }
      if (title === "Malformed response") {
        return Response.json({
          spreadsheetId: 7, spreadsheetUrl: "https://docs.google.com/spreadsheets/d/sheet_malformed/edit",
          properties: { title },
        });
      }
      return Response.json({
        spreadsheetId: "sheet_created_1",
        spreadsheetUrl: "https://docs.google.com/spreadsheets/d/sheet_created_1/edit",
        properties: {
          title, locale: "en_US", autoRecalc: "ON_CHANGE", timeZone: "America/Los_Angeles",
          defaultFormat: { backgroundColorStyle: { rgbColor: { red: 1, green: 1, blue: 1 } } },
          spreadsheetTheme: { primaryFontFamily: "Arial" },
        },
        sheets: [{ properties: { sheetId: 0, title: "Sheet1", index: 0, sheetType: "GRID" } }],
        namedRanges: [], developerMetadata: [], dataSources: [], dataSourceSchedules: [],
      }, { headers: { "x-request-id": "sheets-create-request-1" } });
    }
    if (url.origin === "https://sheets.googleapis.com") {
      return Response.json({ spreadsheetId: "sheet_1", tableRange: "Sheet1!A1:A1", updates: { updatedCells: 1, updatedColumns: 1, updatedRange: "Sheet1!A2", updatedRows: 1 } }, { headers: { "x-request-id": "sheets-request-1" } });
    }
    return Response.json({ error: "unexpected_mock_target" }, { status: 599 });
  },
};
