use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde::{Deserialize, Serialize};

pub const GOOGLE_CALENDAR_BASE_URL: &str = "https://www.googleapis.com";
pub const GOOGLE_CALENDAR_READONLY_SCOPE: &str =
    "https://www.googleapis.com/auth/calendar.readonly";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GoogleCalendarEventOrderBy {
    StartTime,
    Updated,
}

impl GoogleCalendarEventOrderBy {
    pub const fn as_google_api_value(self) -> &'static str {
        match self {
            Self::StartTime => "startTime",
            Self::Updated => "updated",
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GoogleCalendarEventsListQuery {
    pub fields: Option<String>,
    pub max_results: Option<u32>,
    pub order_by: Option<GoogleCalendarEventOrderBy>,
    pub page_token: Option<String>,
    pub show_deleted: Option<bool>,
    pub single_events: Option<bool>,
    pub sync_token: Option<String>,
    pub time_max: Option<String>,
    pub time_min: Option<String>,
    pub updated_min: Option<String>,
}

impl GoogleCalendarEventsListQuery {
    pub fn to_query_pairs(&self) -> Vec<(String, String)> {
        let mut pairs = Vec::new();
        push_optional_string(&mut pairs, "timeMin", self.time_min.as_deref());
        push_optional_string(&mut pairs, "timeMax", self.time_max.as_deref());
        push_optional_string(&mut pairs, "updatedMin", self.updated_min.as_deref());
        push_optional_u32(&mut pairs, "maxResults", self.max_results);
        push_optional_string(&mut pairs, "pageToken", self.page_token.as_deref());
        push_optional_string(&mut pairs, "syncToken", self.sync_token.as_deref());
        if let Some(order_by) = self.order_by {
            pairs.push((
                "orderBy".to_string(),
                order_by.as_google_api_value().to_string(),
            ));
        }
        push_optional_bool(&mut pairs, "singleEvents", self.single_events);
        push_optional_bool(&mut pairs, "showDeleted", self.show_deleted);
        push_optional_string(&mut pairs, "fields", self.fields.as_deref());
        pairs
    }
}

pub fn calendar_events_path(calendar_id: &str) -> String {
    format!(
        "/calendar/v3/calendars/{}/events",
        encode_path_segment(calendar_id)
    )
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoogleCalendarEventsListResponse {
    #[serde(default)]
    pub items: Vec<GoogleCalendarEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_sync_token: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoogleCalendarEvent {
    pub id: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<GoogleCalendarAttachment>,
    #[serde(default)]
    pub attendees: Vec<GoogleCalendarPerson>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conference_data: Option<GoogleCalendarConferenceData>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end: Option<GoogleCalendarEventDateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hangout_link: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub html_link: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub organizer: Option<GoogleCalendarPerson>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<GoogleCalendarEventDateTime>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoogleCalendarAttachment {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoogleCalendarConferenceData {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conference_id: Option<String>,
    #[serde(default)]
    pub entry_points: Vec<GoogleCalendarConferenceEntryPoint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conference_solution: Option<GoogleCalendarConferenceSolution>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoogleCalendarConferenceEntryPoint {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_point_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meeting_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub passcode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoogleCalendarConferenceSolution {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoogleCalendarEventDateTime {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub date: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub date_time: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_zone: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GoogleCalendarPerson {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_status: Option<String>,
    #[serde(default, rename = "self", skip_serializing_if = "Option::is_none")]
    pub self_value: Option<bool>,
}

fn push_optional_bool(pairs: &mut Vec<(String, String)>, name: &str, value: Option<bool>) {
    if let Some(value) = value {
        pairs.push((name.to_string(), value.to_string()));
    }
}

fn push_optional_string(pairs: &mut Vec<(String, String)>, name: &str, value: Option<&str>) {
    if let Some(value) = value {
        pairs.push((name.to_string(), value.to_string()));
    }
}

fn push_optional_u32(pairs: &mut Vec<(String, String)>, name: &str, value: Option<u32>) {
    if let Some(value) = value {
        pairs.push((name.to_string(), value.to_string()));
    }
}

fn encode_path_segment(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calendar_events_list_query_has_no_default_posture() {
        assert!(GoogleCalendarEventsListQuery::default()
            .to_query_pairs()
            .is_empty());
    }

    #[test]
    fn calendar_events_list_query_serializes_only_explicit_options() {
        let query = GoogleCalendarEventsListQuery {
            fields: Some("items(id,summary)".to_string()),
            max_results: Some(25),
            order_by: Some(GoogleCalendarEventOrderBy::Updated),
            page_token: Some("page-token-2".to_string()),
            show_deleted: Some(false),
            single_events: Some(true),
            sync_token: Some("sync-token-9".to_string()),
            time_max: Some("2026-04-17T00:00:00Z".to_string()),
            time_min: Some("2026-04-16T00:00:00Z".to_string()),
            updated_min: Some("2026-04-16T12:00:00Z".to_string()),
        };

        assert_eq!(
            query.to_query_pairs(),
            vec![
                ("timeMin".to_string(), "2026-04-16T00:00:00Z".to_string()),
                ("timeMax".to_string(), "2026-04-17T00:00:00Z".to_string()),
                (
                    "updatedMin".to_string(),
                    "2026-04-16T12:00:00Z".to_string(),
                ),
                ("maxResults".to_string(), "25".to_string()),
                ("pageToken".to_string(), "page-token-2".to_string()),
                ("syncToken".to_string(), "sync-token-9".to_string()),
                ("orderBy".to_string(), "updated".to_string()),
                ("singleEvents".to_string(), "true".to_string()),
                ("showDeleted".to_string(), "false".to_string()),
                ("fields".to_string(), "items(id,summary)".to_string()),
            ]
        );
    }

    #[test]
    fn calendar_events_path_encodes_calendar_id() {
        assert_eq!(
            calendar_events_path("team transcripts@example.com"),
            "/calendar/v3/calendars/team%20transcripts%40example%2Ecom/events"
        );
    }

    #[test]
    fn calendar_events_response_deserializes_realistic_payload() {
        let response: GoogleCalendarEventsListResponse = serde_json::from_str(
            r#"{
                "items": [
                    {
                        "id": "meeting-event-123",
                        "status": "confirmed",
                        "eventType": "default",
                        "summary": "Weekly staff sync",
                        "description": "Transcript will be filed after the meeting.",
                        "location": "Conference Room A",
                        "htmlLink": "https://www.google.com/calendar/event?eid=meeting-event-123",
                        "hangoutLink": "https://meet.google.com/abc-defg-hij",
                        "updated": "2026-04-16T18:27:03.000Z",
                        "attachments": [
                            {
                                "fileId": "drive-file-1",
                                "fileUrl": "https://drive.google.com/file/d/drive-file-1/view",
                                "title": "Agenda",
                                "mimeType": "application/pdf"
                            }
                        ],
                        "organizer": {
                            "email": "owner@example.com",
                            "displayName": "Meeting Owner",
                            "self": true
                        },
                        "attendees": [
                            {
                                "email": "owner@example.com",
                                "displayName": "Meeting Owner",
                                "responseStatus": "accepted",
                                "self": true
                            },
                            {
                                "email": "guest@example.com",
                                "displayName": "Guest",
                                "responseStatus": "tentative"
                            }
                        ],
                        "conferenceData": {
                            "conferenceId": "abc-defg-hij",
                            "conferenceSolution": {
                                "name": "Google Meet"
                            },
                            "entryPoints": [
                                {
                                    "entryPointType": "video",
                                    "uri": "https://meet.google.com/abc-defg-hij",
                                    "label": "meet.google.com/abc-defg-hij"
                                },
                                {
                                    "entryPointType": "phone",
                                    "uri": "tel:+15551234567",
                                    "meetingCode": "5551234567",
                                    "passcode": "321654"
                                }
                            ]
                        },
                        "start": {
                            "dateTime": "2026-04-16T11:00:00-07:00",
                            "timeZone": "America/Los_Angeles"
                        },
                        "end": {
                            "dateTime": "2026-04-16T11:45:00-07:00",
                            "timeZone": "America/Los_Angeles"
                        }
                    }
                ],
                "nextSyncToken": "sync-token-123"
            }"#,
        )
        .expect("calendar response should deserialize");

        assert_eq!(response.next_sync_token.as_deref(), Some("sync-token-123"));
        assert_eq!(response.items.len(), 1);

        let event = &response.items[0];
        assert_eq!(event.id, "meeting-event-123");
        assert_eq!(event.summary.as_deref(), Some("Weekly staff sync"));
        assert_eq!(event.attendees.len(), 2);
        assert_eq!(
            event.conference_data.as_ref().and_then(|data| data.conference_id.as_deref()),
            Some("abc-defg-hij")
        );
        assert_eq!(event.attachments.len(), 1);
        assert_eq!(
            event
                .start
                .as_ref()
                .and_then(|start| start.date_time.as_deref()),
            Some("2026-04-16T11:00:00-07:00")
        );
    }
}
